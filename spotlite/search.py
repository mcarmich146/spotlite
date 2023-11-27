# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.
# 
# Class Searcher Methods
#   search_archive
#   save_tiles

from typing import Tuple, Dict, Optional, List, Type
from shapely.geometry import Polygon, Point, box
import os
import shutil
import sys
import geopandas as gpd
import pandas as pd
from pystac import ItemCollection
from pystac_client import Client
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import requests
from geopy.distance import distance

logger = logging.getLogger(__name__)
tiles_gdf = None
    
class Searcher:
    def __init__(self, key_id="", key_secret=""):
        # Assigning default values to instance attributes
        self.stac_api_url = "https://api.satellogic.com/archive/stac"
        self.key_id = key_id
        self.key_secret = key_secret
        self.cloud_threshold = 30
        self.min_product_version = "1.0.0"
        self.min_tile_coverage_percent = 0.01
        self.valid_pixel_percent_for_basemap = 100
        self.is_internal_to_satl = False
        self._param = None  # Initialize _param for the property

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    # Main function to handle multi-threading based on date ranges
    def search_archive(self, aoi, start_date, end_date):
        # Generate date chunks
        date_chunks = list(self._date_range_chunks(start_date, end_date))

        # Container for all the results
        all_results = []

        # Use ThreadPoolExecutor to run searches in parallel
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Create a dictionary to hold futures
            future_to_date = {
                executor.submit(self._search_with_dates, aoi, chunk_start, chunk_end): (chunk_start, chunk_end)
                for chunk_start, chunk_end in date_chunks
            }

            # Collect the results as they complete
            for future in as_completed(future_to_date):
                try:
                    result = future.result()
                    if result and len(result) > 0:
                        all_results.append(result)  # Append each result assuming it found tiles.
                        # logger.debug(f"Appending Tiles: {len(result)}")
                except Exception as exc:
                    date_range = future_to_date[future]
                    logger.error(f"Search for range {date_range} generated an exception: {exc}")

        all_gdfs = []
        epsg_code = None

        logger.info(f"Processing {len(all_results)} num of items groups")
        for items in all_results:
            if items and len(items) > 0:
                # If first time through then epsg_code is None meaning to use whatever CRS is in that tile group
                gdf = self._setup_GDF(items, epsg_code) 
                all_gdfs.append(gdf)
                epsg_code = gdf.crs #set the epsg_code to the GDF.crs for future tile groups.
                # logger.info(f"Using EPSG:{epsg_code} for all tiles.")
            else:
                # Option 1: Log the absence of data
                logger.debug("No items found for a date chunk, skipping...")
        
        # Check if all_gdfs is empty
        if not all_gdfs:
            logger.warning("No data found during search.")
            return pd.DataFrame(), 0, 0  # Returning an empty DataFrame and zeros

        # Combine all GeoDataFrames into one
        tiles_gdf = pd.concat(all_gdfs, ignore_index=True)

        # Transform the result to a geodataframe to easily manipulate and explore the data
        # grouped = group_items_into_GPDF(combined_items)
        grouped = self._group_by_capture(tiles_gdf)
        
        # Print the results to the log.
        num_captures = 0
        for (capture_date, outcome_id), group in grouped:
            tile_count = len(group)
            # Attempt to get the cloud cover information from the 'eo:cloud_cover' property.
            cloud_cover_mean = None
            if 'eo:cloud_cover' in group.columns:
                cloud_cover_mean = group["eo:cloud_cover"].mean()
            else:
                cloud_cover_mean = 101
                logger.info("Column 'eo:cloud_cover' doesn't exist, Setting CC to 101!")
            
            # Grab the first tile's product version
            product_version = group.iloc[0]['satl:product_version']
            
            logger.warning(f"Capture Date: {capture_date}, Outcome ID: {outcome_id}, Tile Count: {tile_count}, Cloud Cover: {cloud_cover_mean:.0f}%, Prod. Ver.: {product_version}")
            num_captures = num_captures + 1

        # Return the search results
        return tiles_gdf, len(tiles_gdf), num_captures
        
    def save_tiles(self, tiles_gdf, output_dir=None):

        if tiles_gdf is None:
            logger.warning("Search Output Not Initialized.  Run search_archive first.")
            return False
        if tiles_gdf.empty:
            logger.warning("No items found to be animated.")
            return False
        
        grouped_items_GPDF = self._group_by_capture(tiles_gdf)

        # Create a global directory for all tiles
        now = datetime.now().strftime("%y-%m-%dT%H%M%S")
        
        directory_name = ""
        if output_dir is None:
            directory_name = f"images/Tiles_{now}"
        else:
            directory_name = output_dir
        self._ensure_dir(directory_name)

        # for index, (capture_date, outcome_id), group in grou[]ped_items_GPDF:
        for index, ((capture_date, outcome_id), group) in enumerate(grouped_items_GPDF):
            logger.debug(f"Processing Group: {index+1}")
            logger.debug(f"Len of Group: {len(group)}")
            
            tile_number = 1
            
            for id, tile_gdf in group.iterrows():
                logger.debug(f"Processing Tile: {id+1}")
                # Check the tile cloud cover and reject if cloudy.
                cloud_cover = tile_gdf['eo:cloud_cover']
                logger.debug(f"Tile_GDF: {tile_gdf}")
                if cloud_cover is None:
                    logger.warning("Cloud cover information missing. Skipping tile...")
                    continue
                
                # Check the tile cloud cover and reject if cloudy.
                if cloud_cover > self.cloud_threshold:
                    logger.info(f"Tile Rejected With Cloud Cover Of: {cloud_cover:.0f}")
                    continue
                url = tile_gdf['analytic_url']
                capture_date_str = capture_date.strftime("%Y-%m-%dT%H%M%SZ")
                tile_filename = os.path.join(directory_name, f"QuickView_Tile_CD_{capture_date_str}_ID_{tile_number}.tif")
                logger.debug(f"Tile_filename: {tile_filename}")
                logger.debug(f"Analytic URL: {url}")

                # Download and save the tile
                try:
                    with requests.get(url, stream=True) as r:
                        r.raise_for_status()
                        with open(tile_filename, 'wb') as f:
                            shutil.copyfileobj(r.raw, f)
                except Exception as e:
                    logger.error(f"Failed to save tile: {e}")
                    continue

                
                tile_number += 1

            self._show_progress_bar(index+1, len(grouped_items_GPDF))
        self._show_progress_bar(len(grouped_items_GPDF), len(grouped_items_GPDF))
        return True

    def create_aois_from_points(
        self,
        points: List[Dict[str, float]],
        width: float) -> Tuple[List[Polygon], List[Point]]:
        """Create a list of AOIs based on a Points list."""

        aois_list = []
        points_list = []
        for point in points:
            lat, lon = point['lat'], point['lon']
            # logger.warning(f"Lat, Long, Width: {lat}, {lon}, {width}")
            aoi = self._create_bounding_box(lat, lon, width)

            # Add the bounding box polygon to the list of search areas.
            aois_list.append(aoi)
            point_obj = Point(lon,lat)
            points_list.append(point_obj)

        # Return both the master map and the list of AOIs
        return aois_list, points_list

    def _create_bounding_box(self,
                             center_lat: float,
                            center_lon: float,
                            width_km: float = 3) -> Type[Polygon]:  # Tuple[float, float, float, float]:
        """Create bounding box based on center and width in 16:9 AR for presentations."""

        # Calculate the height based on the width to maintain a 16:9 aspect ratio.
        height_km = width_km * 9 / 16

        # Calculate the deltas in km for each direction
        north_point = distance(
            kilometers=height_km/2).destination(point=(center_lat, center_lon), bearing=0)
        south_point = distance(
            kilometers=height_km/2).destination(point=(center_lat, center_lon), bearing=180)
        east_point = distance(
            kilometers=width_km/2).destination(point=(center_lat, center_lon), bearing=90)
        west_point = distance(
            kilometers=width_km/2).destination(point=(center_lat, center_lon), bearing=270)

        # Extract the latitude and longitude from each point
        north_lat, _ = north_point.latitude, north_point.longitude
        south_lat, _ = south_point.latitude, south_point.longitude
        _, east_lon = east_point.latitude, east_point.longitude
        _, west_lon = west_point.latitude, west_point.longitude

        # Create the bounding box
        bbox = box(west_lon, south_lat, east_lon, north_lat)

        return bbox

    def _connect_to_archive(self):
        if self.is_internal_to_satl == True:
            logging.debug("Using Internal Archive Access.")
            archive = Client.open(self.internal_stac_api_url)
        else:
            API_KEY_ID = self.key_id
            API_KEY_SECRET = self.key_secret
            STAC_API_URL = self.stac_api_url
            logger.debug("Using Credentials Archive Access")
            headers = {"authorizationToken":f"Key,Secret {API_KEY_ID},{API_KEY_SECRET}"}
            logger.debug(f"headers: {headers}")
            
            archive = Client.open(STAC_API_URL, headers=headers)
            response = requests.get(STAC_API_URL, headers=headers)  # include your auth headers here
            logger.debug(response.status_code)
        return archive

    # Function to split the date range into two-month chunks
    def _date_range_chunks(self, start_date, end_date, chunk_size_days=14):
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        delta = timedelta(days=chunk_size_days)  # Roughly two months

        while start < end:
            chunk_end = min(start + delta, end)
            yield start.isoformat(), chunk_end.isoformat()
            start = chunk_end

    # Modified search function to accept start and end dates
    def _search_with_dates(self, aoi, start_date, end_date):
        try:
            # Connect To The Archive
            archive = self._connect_to_archive()
            logger.debug("Connected To Archive")

            if not archive:
                logger.error("Failed to connect to archive.")
                return None, 0, 0
            logger.debug(f"Start-End: {start_date}-{end_date}")
            items = archive.search(
                intersects=aoi,
                collections=["quickview-visual"],
                datetime=f"{start_date}/{end_date}",
                query={"satl:product_name": {"eq": "QUICKVIEW_VISUAL"}},
            ).item_collection()

            logger.debug(f"Search Complete for period: {start_date} to {end_date}!")

            if items is None:
                logger.debug(f"No results returned for period: {start_date} to {end_date}")
                return None

            if not isinstance(items, ItemCollection):  
                logger.error(f"Unexpected type returned: {type(items)}")
                return None

            if len(items) == 0:
                logger.debug(f"Search returned an empty collection for period: {start_date} to {end_date}")
                return None
            logger.debug(f"Num Tiles Found: {len(items)}")

            return items

        except Exception as e:
            logger.error(f"Error during search for period: {start_date} to {end_date}: {e}")
            return None


    def _setup_GDF(self, items, epsg_code_input=None):
        # Check if items is None or empty
        if items is None or len(items) == 0:
            logger.error("Error: Trying To Group Empty Items!")
            return False
            
        # Check for the first item and its properties
        first_item = next(iter(items), None)
        if first_item is not None:
            first_epsg_code_number = first_item.properties.get('proj:epsg', None)
            first_epsg_code = f"epsg:{first_epsg_code_number}"
            # logger.info(f"epsg_code_input: {epsg_code_input}")
            # logger.info(f"first_epsg_code: {first_epsg_code}")
        else:
            print("The collection is empty.")
            return False

        gdfs = []

        # Convert the ItemCollection to a dictionary array and make sure the CRS is handled for all tiles.

        for item in items:
            epsg_code_number = item.properties.get('proj:epsg', None)
            epsg_code = f"epsg:{epsg_code_number}"
            
            if not epsg_code:
                logger.warning("'proj:epsg' not found in item's properties.")
                continue

            # if the epsg_code_input is set we should use it as the code otherwise use standard code WGS84.
            if epsg_code_input is not None: # If a epsg code is provided as an arg
                target_crs = epsg_code_input
            else:
                target_crs = first_epsg_code

            feature = item.to_dict()
            gdf = gpd.GeoDataFrame.from_features([feature], crs=f"{target_crs}")

            gdf['id'] = item.id
            gdf['capture_date'] = pd.to_datetime(item.datetime)
            gdf['capture_date'] = gdf['capture_date'].dt.tz_localize(None)
            gdf['geometry'] = gdf['geometry'].apply(lambda x: x.buffer(0))
            gdf['data_age'] = (datetime.utcnow() - gdf['capture_date']).dt.days  # Using utcnow
            gdf['preview_url'] = item.assets["preview"].href
            gdf['thumbnail_url'] = item.assets["thumbnail"].href
            gdf['analytic_url'] = item.assets["analytic"].href
            gdf['outcome_id'] = item.properties['satl:outcome_id']
            gdf['valid_pixel_percent'] = item.properties['satl:valid_pixel']
            
            gdfs.append(gdf)

        # Combine all reprojected GeoDataFrames
        combined_gdf = pd.concat(gdfs, ignore_index=True)

        # Count the number of tiles in each 'grid:code'
        tile_counts = combined_gdf['grid:code'].value_counts().reset_index()
        tile_counts.columns = ['grid:code', 'image_count']

        # Join this back to the original GeoDataFrame
        combined_gdf = pd.merge(combined_gdf, tile_counts, on='grid:code', how='left')
        
        return combined_gdf

    def _group_by_capture(self, gdf):
        # Grouping the data
        grouped = gdf.groupby([gpd.pd.Grouper(key="capture_date", freq="S"), "satl:outcome_id"])
        return grouped # A GeoPanadasDF
    
    def _show_progress_bar(self, iteration, total, bar_length=50):
        progress = float(iteration) / float(total)
        arrow = '-' * int(round(progress * bar_length) - 1)
        spaces = ' ' * (bar_length - len(arrow))

        sys.stdout.write(f'\r[{arrow}{spaces}]')
        sys.stdout.flush()

    def _ensure_dir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)


