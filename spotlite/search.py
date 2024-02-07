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
from pandas.core.groupby import DataFrameGroupBy
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
    def search_archive(self, aoi: Polygon, start_date: str, end_date: str):
        search_start_timestamp = datetime.now()

        # Generate date chunks
        date_chunks = list(self._date_range_chunks(start_date, end_date))

        num_chunks = len(date_chunks)

        # Container for all the results
        all_results = []

        self._show_progress_bar(0, num_chunks)
        # Use ThreadPoolExecutor to run searches in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Create a dictionary to hold futures
            future_to_date = {
                executor.submit(self._search_with_dates, aoi, chunk_start, chunk_end): (chunk_start, chunk_end)
                for chunk_start, chunk_end in date_chunks
            }

            index = 0
            # Collect the results as they complete
            for future in as_completed(future_to_date):
                try:
                    result = future.result()
                    if result and len(result) > 0:
                        all_results.append(result)  # Append each result assuming it found tiles.
                        self._show_progress_bar(index+1, num_chunks)
        
                except Exception as exc:
                    date_range = future_to_date[future]
                    logger.error(f"Search for range {date_range} generated an exception: {exc}")
                index += 1
            self._show_progress_bar(num_chunks, num_chunks)
            print()

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
            return pd.DataFrame()  # Returning an empty DataFrame and zeros

        # Combine all GeoDataFrames into one
        tiles_gdf = pd.concat(all_gdfs, ignore_index=True)
 
        search_end_timestamp = datetime.now()
        total_search_duration = search_end_timestamp - search_start_timestamp
        logger.warning(f"Total Search Duration: {total_search_duration}")

        # Return the search results
        return tiles_gdf

    def search_archive_for_outcome_id(self, outcome_id: str):
        try:
            # Connect To The Archive
            archive = self._connect_to_archive()
            logger.debug("Connected To Archive")

            if not archive:
                logger.error("Failed to connect to archive.")
                return None

            items = archive.search(
                collections=["quickview-visual"],
                query={"satl:outcome_id": {"eq":outcome_id}},
            ).item_collection()

            if items is None or len(items) == 0:
                logger.debug(f"No results returned for Outcome_ID: {outcome_id}")
                return None

            if not isinstance(items, ItemCollection):  
                logger.error(f"Unexpected type returned: {type(items)}")
                return None

            logger.debug(f"Num Tiles Found: {len(items)}")

            tiles_gdf = self._setup_GDF(items) 
            return tiles_gdf

        except Exception as e:
            logger.error(f"Error during search for Outcome_ID: {outcome_id}: {e}")
            return None
        
    def _connect_to_archive(self):
        try:
            
            API_KEY_ID = self.key_id
            API_KEY_SECRET = self.key_secret
            STAC_API_URL = self.stac_api_url
            headers = {"authorizationToken": f"Key,Secret {API_KEY_ID},{API_KEY_SECRET}"}
            
            logging.debug("Using Credentials Archive Access with headers: %s", headers)
            archive = Client.open(STAC_API_URL, headers=headers)
            # Test connection
            response = requests.get(STAC_API_URL, headers=headers)
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            logging.debug("Connection test successful with status code %s", response.status_code)

            # Print out the available collections.
            for collection in archive.get_all_collections():
                logging.debug(f"Collection ID: {collection.id}, Title: {collection.title}")
            return archive

        except requests.exceptions.HTTPError as http_err:
            logging.error("HTTP error occurred: %s", http_err)
            return None
        except Exception as e:
            logging.error("Error occurred while connecting to archive: %s", e)
            return None

    # Function to split the date range into two-week chunks
    def _date_range_chunks(self, start_date: str, end_date: str, chunk_size_days=30):
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        delta = timedelta(days=chunk_size_days)  # default two weeks

        while start < end:
            chunk_end = min(start + delta, end)
            yield start.isoformat(), chunk_end.isoformat()
            start = chunk_end

    # Modified search function to accept start and end dates
    def _search_with_dates(self, aoi, start_date, end_date):
        start_timestamp = datetime.now()
        try:
            # Connect To The Archive
            archive = self._connect_to_archive()
            logger.debug("Connected To Archive")

            db_connect_now = datetime.now()
            db_duration = db_connect_now - start_timestamp
            # logger.debug(f"DB CONNECT DURATION: {db_duration}")
            if not archive:
                logger.error("Failed to connect to archive.")
                return None
            logger.debug(f"Start-End: {start_date}-{end_date}")
            items = archive.search(
                intersects=aoi,
                collections=["quickview-visual"],
                datetime=f"{start_date}/{end_date}",
                # query={"satl:product_name": {"eq": "QUICKVIEW_VISUAL"}},
            ).item_collection()

            logger.debug(f"Search Complete for period: {start_date} to {end_date}!")
            search_done_now = datetime.now()
            logger.debug(f"Search Duration: {search_done_now - db_connect_now}")
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

    def _show_progress_bar(self, iteration, total, bar_length=50):
        progress = float(iteration) / float(total)
        arrow = '-' * int(round(progress * bar_length) - 1)
        spaces = ' ' * (bar_length - len(arrow))

        sys.stdout.write(f'\r[{arrow}{spaces}]')
        sys.stdout.flush()

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