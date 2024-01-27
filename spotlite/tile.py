# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite and serves as a utilities class for visualization
# and map functions.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.
#
# Functions:
#   save_and_animate_tiles
#   age_heatmap
#   count_heatmap
#   cloud_heatmap
#   update_map_with_tiles
#   update_map_with_footprints
#   create_folium_map
#   create_choropleth_map
#   filter_tiles
#   filter_and_sort_tiles
#   create_folium_basemap
#   create_aois_from_points
#   get_tiles

from typing import Tuple, Dict, Optional, List, Type
import os
import math
from io import BytesIO
import numpy as np
import geopandas as gpd
from pandas.core.groupby import DataFrameGroupBy
import pandas as pd
import warnings
from rasterio.errors import NotGeoreferencedWarning
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject, Resampling
from rasterio.transform import from_origin
from rasterio.io import MemoryFile
import requests
import shutil
import sys
from rasterio.merge import merge
import plotly.express as px
from shapely import Point
from shapely.ops import unary_union
from shapely.geometry import shape, box, Polygon
from geopy.distance import distance
from PIL import Image
from datetime import datetime
import imageio
from PIL import ImageDraw, ImageFont, Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from packaging import version
import logging
import plotly.graph_objs as go
import branca.colormap as cm
import folium
from folium import raster_layers
from .search import Searcher 

logger = logging.getLogger(__name__)

class TileManager:
    def __init__(self, key_id="", key_secret=""):
        # Set Defaults
        self.key_id = key_id
        self.key_secret = key_secret
        self.period_between_frames = 2
        self.min_product_version = "1.0.0"
        self.min_tile_coverage_percent = 0.01
        self.valid_pixel_percent_for_basemap = 100
        self.cloud_threshold = 30
        self._param = None

        self.searcher = Searcher(self.key_id, self.key_secret)

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    def animate_tile_stack(self, tiles_gdf, bbox_aoi, font=None):
        abs_output_animation_filename = None
        if tiles_gdf.empty:
            logger.warning("No items found to be animated.")
            return None

        fnames = []
        grouped = self.group_by_outcome_id(tiles_gdf)

        # if not font is specified, use default font
        if font is None:
            font = ImageFont.load_default()

        global max_tile_count
        max_tile_count = max(len(group) for _, group in grouped)

        # Use ThreadPoolExecutor to process groups in parallel
        with ThreadPoolExecutor() as executor:
            # Create a list to hold the Future objects
            futures = []

            # Iterate over each group and submit it for processing
            for outcome_id, group_df in grouped:
                # Use first tile capture_time
                capture_date = group_df.iloc[0]['capture_date']
                
                # Submit the group to the process_group function
                future = executor.submit(self._process_group, capture_date, outcome_id, group_df)
                # Add the future to the list
                futures.append(future)

            # Iterate over the futures as they complete
            for future in as_completed(futures):
                # Get the result from the future
                result = future.result()
                # If there's a result, add it to the fnames list
                if result:
                    fnames.append(result)

        animate_images = "y" #input("Animate stack of tiles? (y/n):") or "y"

        if animate_images == "y":
            try:
                logger.info("Animating Images...")
                seconds_between_frames = self.period_between_frames
                now = datetime.now().strftime("%Y%m%dT%H%M%S")
                output_animation_filename = f'images/Stack_Animation_Video_{now}.GIF'

                self._create_animation_from_files(fnames, output_animation_filename, seconds_between_frames, bbox_aoi, font)
                # create_before_and_after(fnames)

                # Validate that the file was actually created
                if os.path.isfile(output_animation_filename):
                    abs_output_animation_filename = os.path.abspath(output_animation_filename)
                    abs_output_animation_filename = abs_output_animation_filename.replace('\\', '/')
                    logger.warning(f"Animation File Created: {abs_output_animation_filename}")
                else:
                    logger.warning("Animation file was not created. Check for errors in create_animation_from_files.")
                    return None
            except Exception as e:
                logger.error(f"Error occurred while creating animation: {e}")
                return None

        return abs_output_animation_filename, fnames

    def download_tiles(self, tiles_gdf, output_dir=None):

        if tiles_gdf is None:
            logger.warning("No Tiles Found When Downloading Tiles.")
            return False
        if tiles_gdf.empty:
            logger.warning("No items found to be animated.")
            return False
        
        grouped_items_GPDF = self.group_by_outcome_id(tiles_gdf)

        # Create a global directory for all tiles
        now = datetime.now().strftime("%y-%m-%dT%H%M%S")
        
        directory_name = ""
        if output_dir is None:
            directory_name = f"images/Tiles_{now}"
        else:
            directory_name = output_dir
        self._ensure_dir(directory_name)

        # Go through each tile and write out to the target location
        for index, (outcome_id, group) in enumerate(grouped_items_GPDF):
            logger.warning(f"Processing Capture Num: {index+1}, Outcome_Id: {outcome_id}")
            logger.debug(f"Len of Group: {len(group)}")
            
            tile_number = 1
            
            for id, tile_gdf in group.iterrows():
                self._show_progress_bar(tile_number, len(group))

                logger.debug(f"Processing Tile: {tile_number}")
                # Check the tile cloud cover and reject if cloudy.
                cloud_cover = tile_gdf['eo:cloud_cover']
                # logger.debug(f"Tile_GDF: {tile_gdf}")
                if cloud_cover is None:
                    logger.warning("Cloud cover information missing. Skipping tile...")
                elif cloud_cover > self.cloud_threshold:
                    logger.debug(f"Tile Rejected With Cloud Cover Of: {cloud_cover:.0f}")
                else:
                    url = tile_gdf['analytic_url']
                    logger.debug(f"Tile ID/Analytic URL: {tile_number}/{url}")
                    capture_date_str = tile_gdf['capture_date'].strftime("%Y-%m-%dT%H%M%SZ")
                    tile_filename = os.path.join(directory_name, f"L1B_Tile_CD_{capture_date_str}_ID_{tile_number}.tif")
                    logger.debug(f"Tile_filename: {tile_filename}")

                    # Download and save the tile
                    try:
                        with requests.get(url, stream=True) as r:
                            r.raise_for_status()
                            with open(tile_filename, 'wb') as f:
                                shutil.copyfileobj(r.raw, f)
                    except Exception as e:
                        logger.error(f"Failed to save tile: {e}")

                
                # Increment tile_number at the end of each iteration
                tile_number += 1
            self._show_progress_bar(tile_number, len(group))
            print("\n")
        logger.warning("Tile Download Completed.") #add a new line after the progress bar.
        return True

    def group_by_capture_date(self, gdf: gpd.GeoDataFrame) -> DataFrameGroupBy:
        # Grouping the data by capture_date
        grouped = gdf.groupby([gpd.pd.Grouper(key="capture_date", freq="S"), "satl:outcome_id"])
        return grouped  # A GeoPandas DataFrameGroupBy object

    def group_by_outcome_id(self, gdf: gpd.GeoDataFrame) -> DataFrameGroupBy:
        # Grouping the data by outcome id
        grouped = gdf.groupby("satl:outcome_id")
        return grouped  # A GeoPandas DataFrameGroupBy object
    
    def _show_progress_bar(self, iteration, total, bar_length=50):
        progress = float(iteration) / float(total)
        arrow = '-' * int(round(progress * bar_length) - 1)
        spaces = ' ' * (bar_length - len(arrow))

        sys.stdout.write(f'\r[{arrow}{spaces}]')
        sys.stdout.flush()


    def age_heatmap(self, tiles_gdf: Dict, out_filename: str = None) -> folium.Map:
        """Creates a heat map based on age of data, using a linear color map."""

        # Sort the GeoDataFrame based on data_age, so that less old squares are on top
        tiles_gdf = tiles_gdf.sort_values(by='data_age', ascending=False)

        # Determine the center of your data to set the initial view of the map
        center = tiles_gdf.geometry.unary_union.centroid
        start_coord = (center.y, center.x)

        # Determine the data range for color normalization
        data_age_min, data_age_max = tiles_gdf['data_age'].min(), tiles_gdf['data_age'].max()
        # data_age_min = 0
        # data_age_max = 30 # Basically scale it to a month.
        colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[data_age_min, data_age_max],
                                    vmin=data_age_min, vmax=data_age_max)

        # Create the folium map
        m = folium.Map(location=start_coord, zoom_start=8)

        for idx, row in tiles_gdf.iterrows():
            # Scaling opacity: younger squares more opaque (0.8), older squares less opaque (0.4)
            opacity_scaled = 0.8 - \
                ((row['data_age'] - data_age_min) /
                (data_age_max - data_age_min)) * 0.4

            tooltip_text = f"Age: {row['data_age']}"
            tooltip = folium.Tooltip(tooltip_text)
            folium.Polygon(
                locations=[(lat, lon) for lon, lat in zip(
                    row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
                color=colormap(row['data_age']),
                fill=True,
                fill_color=colormap(row['data_age']),
                fill_opacity=opacity_scaled,
                opacity=opacity_scaled,
                tooltip=tooltip
            ).add_to(m)

        m.add_child(colormap)  # Add the color map legend

        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/ImageAge_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
        m.save(out_filename)  # Save to an HTML file

        return m


    def count_heatmap(self, tiles_gdf: Dict, out_filename: str = None) -> folium.Map:
        """Creates a heat map based on quantity of available data using a linear color map."""

        # Keep only the latest tile for each grid code.
        # Since they are sorted by age with the youngest last, we can drop duplicates except for the last one.
        # All tiles in a gridcode have the same tilecount field.
        tiles_gdf = tiles_gdf.drop_duplicates(subset='grid:code', keep='last')

        # Sort by age so that youngest tiles are last (and thus displayed on top)
        tiles_gdf = tiles_gdf.sort_values(by='data_age', ascending=False)

        # Determine the center of your data to set the initial view of the map
        center = tiles_gdf.geometry.unary_union.centroid
        start_coord = (center.y, center.x)

        # Determine the data range for color normalization
        count_min, count_max = tiles_gdf['image_count'].min(), tiles_gdf['image_count'].max()
        colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[count_min, count_max],
                                vmin=count_min, vmax=count_max)

        logging.info(f"ImageCountMin: {tiles_gdf['image_count'].min()}, Max: {tiles_gdf['image_count'].max()}")

        # Create the folium map
        m = folium.Map(location=start_coord, zoom_start=8, tiles='cartodbdark_matter')

        # Add polygons to the map
        for idx, row in tiles_gdf.iterrows():
            tooltip_text = f"Image Count: {row['image_count']}"
            tooltip = folium.Tooltip(tooltip_text)

            folium.Polygon(
                locations=[(lat, lon) for lon, lat in zip(row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
                color=colormap(row['image_count']),
                fill=True,
                fill_color=colormap(row['image_count']),
                fill_opacity=0.7,  # Feel free to scale this as needed
                opacity=0.7,
                tooltip=tooltip
            ).add_to(m)

        m.add_child(colormap)  # Add the color map legend
        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/ImageCount_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
        m.save(out_filename)  # Save to an HTML file

        return m

    def cloud_heatmap(self, tiles_gdf: Dict, existing_fig: go.Figure = None, out_filename: str = None) -> go.Figure:
        """Creates a heat map based on cloud coverage in available data using a linear color map."""

        if tiles_gdf.empty:
            logger.warning("No items found.")
            return None  # or however you want to handle an empty response

        # Reset index to have 'id' as a column for px.choropleth_mapbox
        tiles_gdf.reset_index(inplace=True)

        # Calculate the total bounds of the GeoDataFrame
        minx, miny, maxx, maxy = tiles_gdf.total_bounds

        # Calculate the midpoint of the bounds
        center_x = (maxx + minx) / 2
        center_y = (maxy + miny) / 2

        # Create a Point instance for the approximate centroid
        centroid = Point(center_x, center_y)

        # # Filter tiles by cloud cover
        # cloud_filtered_tiles_gdf = tiles_gdf[tiles_gdf['eo:cloud_cover']
        #                                     <= self.cloud_threshold].copy()
        cloud_filtered_tiles_gdf = tiles_gdf

        # Rest of your code remains the same
        cloud_filtered_tiles_gdf.sort_values(by='data_age', ascending=True, inplace=True)

        cloud_filtered_tiles_gdf.drop_duplicates(subset='grid:code', keep='first', inplace=True)

        # Create figure if not provided
        if existing_fig is None:
            fig = px.choropleth_mapbox(
                cloud_filtered_tiles_gdf,
                geojson=cloud_filtered_tiles_gdf.geometry.__geo_interface__,
                locations=cloud_filtered_tiles_gdf.index,
                color="eo:cloud_cover",
                hover_data=['capture_date', 'outcome_id', 'eo:cloud_cover'],
                center={'lat': centroid.y, 'lon': centroid.x},
                zoom=6
            )
            fig.update_traces(marker_line_width=0)
            fig.update_layout(
                width=1200,
                height=700,
                mapbox_style="carto-positron"
            )
        else:
            fig = existing_fig
            new_trace = go.Choroplethmapbox(
                geojson=cloud_filtered_tiles_gdf.geometry.__geo_interface__,
                locations=cloud_filtered_tiles_gdf.index,
                z=cloud_filtered_tiles_gdf['eo:cloud_cover']
            )
            fig.add_trace(new_trace)

        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/CloudCover_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"

        fig.write_html(out_filename)
        return fig

    def update_map_with_tiles(  self,
                                folium_map_obj: folium.Map,
                                tiles_gdf: gpd.GeoDataFrame,
                                animation_filename: str,
                                aoi_bbox: Polygon) -> folium.Map:
        """Update a map with new folium polygon objects."""

        if tiles_gdf.empty:
            print("No items found.")
            return None  # or however you want to handle an empty response

        grouped = self.group_by_outcome_id(tiles_gdf)
        # Iterating through grouped data
        for outcome_id, group in grouped:
            cloud_cover_mean = int(round(group['eo:cloud_cover'].mean()))
            num_rows = len(group)
            mid_index = num_rows // 2  # Integer division to get the middle index
            if num_rows == 1:
                mid_index = 0 

            current_row_ix = 0
            for index, row in group.iterrows():
                capture_date = group.iloc[0]['capture_date']
                geometry = row['geometry']
                if geometry.geom_type == 'Polygon':
                    coords = [[lat, lon] for lon, lat in list(geometry.exterior.coords)]

                    # Add the Polygon to the map
                    folium.Polygon(
                        locations=coords,
                        tooltip=f"CD:{row['capture_date']} CC:{cloud_cover_mean}% OI:{outcome_id}.",
                        color='red',
                        fill=True,
                        fill_color='red',
                        fill_opacity=0.01
                    ).add_to(folium_map_obj)

                    # Add a marker at the centroid of the polygon at the middle index of the group
                    if current_row_ix == mid_index:
                        centroid = geometry.centroid
                        folium.Marker(
                            [centroid.y, centroid.x],
                            popup=f"Capture Date: {capture_date}\nOutcome ID: {outcome_id}\nCloud Cover: {cloud_cover_mean}%"
                        ).add_to(folium_map_obj)

                else:
                    print(f'Unsupported geometry type: {geometry.geom_type}')
                    return False
                current_row_ix += 1
                
        # Create a marker with a popup to display the animation.  If there is no animation then don't add a marker.
        if animation_filename is not None:
            # Calculate centroid of the bbox
            min_lon, min_lat, max_lon, max_lat = aoi_bbox.bounds
            centroid_lon = (min_lon + max_lon) / 2
            centroid_lat = (min_lat + max_lat) / 2
            
            # Create a clickable marker.  Useful for when animations are made and use wants to open them.
            popup_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
            folium.Marker([centroid_lat, centroid_lon], popup_html, parse_html=True).add_to(folium_map_obj)

        return folium_map_obj

    def update_map_with_footprints(  self,
                                folium_map_obj: folium.Map,
                                tiles_gdf: gpd.GeoDataFrame,
                                animation_filename: str,
                                aoi_bbox: Polygon) -> folium.Map:
        """Update a map with new folium polygon objects."""

        if tiles_gdf.empty:
            print("No items found.")
            return None  # or however you want to handle an empty response

        grouped = self.group_by_outcome_id(tiles_gdf)
        # Iterating through grouped data
        for outcome_id, group in grouped:
            cloud_cover_mean = int(round(group['eo:cloud_cover'].mean()))
            combined_footprint = group.geometry.unary_union
            capture_date = group.iloc[0]['capture_date']

            if combined_footprint.geom_type == 'Polygon':
                coords = [[lat, lon] for lon, lat in list(combined_footprint.exterior.coords)]
                # Add the Polygon to the map
                folium.Polygon(
                    locations=coords,
                    tooltip=f"CD:{capture_date} CC:{cloud_cover_mean}% OI:{outcome_id}.",
                    color='red',
                    fill=True,
                    fill_color='red',
                    fill_opacity=0.01
                ).add_to(folium_map_obj)

                # Add a marker at the centroid of the polygon at the middle index of the group
                centroid = combined_footprint.centroid
                folium.Marker(
                        [centroid.y, centroid.x],
                        popup=f"Capture Date: {capture_date}\nOutcome ID: {outcome_id}\nCloud Cover: {cloud_cover_mean}%"
                    ).add_to(folium_map_obj)

            else:
                print(f'Unsupported geometry type: {combined_footprint.geom_type}')
                return False
                        
        # Create a marker with a popup to display the animation.  If there is no animation then don't add a marker.
        if animation_filename is not None:
            # Calculate centroid of the bbox
            min_lon, min_lat, max_lon, max_lat = aoi_bbox.bounds
            centroid_lon = (min_lon + max_lon) / 2
            centroid_lat = (min_lat + max_lat) / 2
            # Create a clickable marker.  Useful for when animations are made and use wants to open them.
            popup_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
            folium.Marker([centroid_lat, centroid_lon], popup_html, parse_html=True).add_to(folium_map_obj)

        return folium_map_obj


    def create_folium_map(
            self,
            points: List[Point],
            aois: List[Polygon]
        ) -> folium.Map:
        """Create a folium map and add all markers and AOIs."""

        if not points:
            raise ValueError("Points list is empty")

        # Set initial location using the first point's coordinates
        initial_lon, initial_lat = points[0].x, points[0].y
        master_map = folium.Map(location=[initial_lat, initial_lon], zoom_start=8)

        for aoi in aois:
            # Create a folium Polygon from AOI and add it to the map
            # folium expects coords in x,y while Shape and GeoJson are in y,x (cartesian)
            folium.Polygon(
                locations=[(lat, lon) for lon, lat in aoi.exterior.coords],
                tooltip="Search Bounding Box"
            ).add_to(master_map)

        return master_map

    def create_choropleth_map(self, aois: List[Polygon]) -> go.Figure:
        """Initialize the 'master' Plotly figure."""
        master_fig = go.Figure()
        all_aoi_shapes = []  # List to keep track of all AOI shapes

        for aoi in aois:
            # Extract centroid coordinates and create a DataFrame
            centroid = aoi.centroid
            df = pd.DataFrame({'lat': [centroid.y], 'lon': [centroid.x]})

            # Create Plotly figure for each AOI
            fig = px.scatter_mapbox(df,
                                    lat='lat',
                                    lon='lon',
                                    mapbox_style="carto-positron",
                                    zoom=8)

            all_aoi_shapes.append(shape(aoi))

            # Extract the traces from the new figure and add them to the 'master' figure
            for trace in fig.data:
                master_fig.add_trace(trace)

        # Calculate the 'global' bounding box if AOIs are present
        if all_aoi_shapes:
            global_bbox = unary_union(all_aoi_shapes).bounds
            minx, miny, maxx, maxy = global_bbox
            zoom_level = self._estimate_zoom_level(minx, miny, maxx, maxy) - 2

            # Update the layout of the 'master' figure
            master_fig.update_layout(
                mapbox={
                    "style": 'carto-positron',
                    "zoom": zoom_level,
                    "center": {
                        "lat": (miny + maxy) / 2,
                        "lon": (minx + maxx) / 2,
                    },
                }
            )

        return master_fig

    def get_tiles_for_outcome_id(self, outcome_id: str):
        """Gets tiles from the STAC Catalog based on the outcome_id for the image"""
        tiles_gdf = self.searcher.search_archive_for_outcome_id(outcome_id)
        return tiles_gdf

    def get_tiles(self, aoi: Polygon, start_date_str: str, end_date: str):
        """Gets tiles from the STAC Catalog"""

        # Search the catalog for tiles.
        tiles_gdf = self.searcher.search_archive(aoi, start_date_str, end_date)
        
        if tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None, 0, 0

        # Group by outcome_id since the tiles in a group have different times according to capture
        grouped = self.group_by_outcome_id(tiles_gdf)

        # Print the results to the log.
        num_captures = 0
        for outcome_id, group in grouped:
            tile_count = len(group)
            # Attempt to get the cloud cover information from the 'eo:cloud_cover' property.
            cloud_cover_mean = None
            if 'eo:cloud_cover' in group.columns:
                cloud_cover_mean = group["eo:cloud_cover"].mean()
            else:
                cloud_cover_mean = 101
                logger.info("Column 'eo:cloud_cover' doesn't exist, Setting CC to 101!")
            
            # Grab the first tile's product version and capture_date
            product_version = group.iloc[0]['satl:product_version']
            capture_date = group.iloc[0]['capture_date']

            logger.warning(f"Capture Date: {capture_date}, Outcome ID: {outcome_id}, Tile Count: {tile_count}, Cloud Cover: {cloud_cover_mean:.0f}%")
            num_captures = num_captures + 1

        # Return the search results
        return tiles_gdf, len(tiles_gdf), num_captures

    def filter_tiles(self, tiles_gdf, cloud_cover=None, valid_pixels_perc=None):
        """Uses the configuration value for cloud_threshold and valid_pixel_percent
           Unless overloaded by the calling parameters."""
        if tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        if cloud_cover is None:
            cloud_cover = self.cloud_threshold

        if valid_pixels_perc is None:
            valid_pixels_perc = self.valid_pixel_percent_for_basemap

        # Filter by cloud coverage and valid pixel percentage
        filtered_tiles_gdf = tiles_gdf[(tiles_gdf['eo:cloud_cover'] <= cloud_cover) &
                                            (tiles_gdf['valid_pixel_percent'] >= valid_pixels_perc)].copy()

        return filtered_tiles_gdf


    def filter_and_sort_tiles(self, tiles_gdf, cloud_cover=None, valid_pixels_perc=None):
        """Filters tiles based on cloud cover and valid pixel percent and then sorts the tiles.
           Then it keeps only the latest tile for use in heatmaps."""
        if tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        filtered_tiles_gdf = self.filter_tiles(tiles_gdf, cloud_cover, valid_pixels_perc)

        # Sort by capture date
        filtered_tiles_gdf.sort_values('capture_date', ascending=False, inplace=True)


        # Group by grid cell and take the first (most recent) record
        most_recent_cloud_free_tiles = filtered_tiles_gdf.groupby('grid:code').first().reset_index()

        return most_recent_cloud_free_tiles

    def create_folium_basemap(self, capture_grouped_tiles_gdf: Dict) -> folium.Map:
        """Create folium basemap as starting point for further updates with heatmaps."""

        if capture_grouped_tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        # Create a folium map
        center = capture_grouped_tiles_gdf.geometry.unary_union.centroid
        folium_map = folium.Map(location=[center.y, center.x], zoom_start=8)

        for idx, row in capture_grouped_tiles_gdf.iterrows():
            coords = [(lat, lon) for lon, lat in zip(*row.geometry.exterior.coords.xy)]
            # Create a tooltip using capture date and outcome_id
            tooltip_text = f"{row['capture_date'].strftime('%Y-%m-%dT%H%M%SZ')}, {row['outcome_id']}"
            tooltip = folium.Tooltip(tooltip_text)
            folium.Polygon(coords, color='blue', weight=1, tooltip=tooltip).add_to(folium_map)

            # Adding image overlay
            bounds = [list(row.geometry.bounds[1::-1]), list(row.geometry.bounds[3:1:-1])]

            image_url = row["thumbnail_url"]
            # logger.debug(f"Thumbnail image_url: {image_url}")
            raster_layers.ImageOverlay(image_url, bounds=bounds).add_to(folium_map)

        return folium_map

    def group_by_capture_date(self, gdf: gpd.GeoDataFrame) -> DataFrameGroupBy:
        # Grouping the data
        grouped = gdf.groupby([gpd.pd.Grouper(key="capture_date", freq="S"), "satl:outcome_id"])
        return grouped  # A GeoPandas DataFrameGroupBy object

    def group_by_outcome_id(self, gdf: gpd.GeoDataFrame) -> DataFrameGroupBy:
        # Grouping the data by outcome id
        grouped = gdf.groupby("satl:outcome_id")
        return grouped  # A GeoPandas DataFrameGroupBy object
    
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
            aoi = self.create_bounding_box(lat, lon, width)

            # Add the bounding box polygon to the list of search areas.
            aois_list.append(aoi)
            point_obj = Point(lon,lat)
            points_list.append(point_obj)

        # Return both the master map and the list of AOIs
        return aois_list, points_list

    def create_bounding_box(self,
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

    def create_preview_jpegs(self, tiles_gdf) -> List:
        """Takes a multi-capture list of tiles and returns a list of preview filenames"""
        if tiles_gdf.empty:
            logger.debug("No items found while creating preview files.")
            return None
        
        output_filenames = []

        now = datetime.now().strftime("%y-%m-%dT%H-%M-%S")

        # Group the tiles by outcome_id
        grouped_GPDF = self.group_by_outcome_id(tiles_gdf)

        self._ensure_dir("images")
        logger.warning("Preparing Previews-Thumbnails")
        # Iterating through grouped data
        for outcome_id, tiles_group in grouped_GPDF:
            capture_date = tiles_group.iloc[0]['capture_date'] #use the first tile in the group.
            
            # Save the image as JPEG
            output_filename = f"images/Preview_{capture_date}_{outcome_id}_{now}.JPEG"
            output_filenames.append(output_filename)
            
            mosaic_image, out_trans = self._mosaic_preview_tiles(tiles_group, output_filename)

        return output_filenames

    def _process_group(self, capture_date, outcome_id, group_df):
        if False == self._is_group_valid(group_df):
            return None  # return None if the group is rejected
        logger.info(f"Spawned Mosaic Process: {capture_date}, {outcome_id}")

        fname = f"CaptureDate_{capture_date.strftime('%Y%m%dT%H%M%S')}_MosaicCreated_{datetime.now().strftime('%Y%m%dT%H%M%S')}.tiff"
        full_path = os.path.join('images', fname)
        self._mosaic_analytic_tiles(group_df, full_path)
        return full_path  # return the filename if the group is processed

    def _estimate_zoom_level(self, minx, miny, maxx, maxy):
        """Calculate the geographic extent."""

        width = maxx - minx
        height = maxy - miny

        # get the larger of the two dimensions
        max_dim = max(width, height)
        # print(f"Max_Dim:{max_dim}")
        # estimate zoom level based on max dimension
        # these thresholds are arbitrary and might need adjustment
        if max_dim > 10:
            zoom_level = 6
        elif max_dim > 5:
            zoom_level = 7
        elif max_dim > 2:
            zoom_level = 8
        elif max_dim > 1:
            zoom_level = 9
        elif max_dim > 0.5:
            zoom_level = 10
        elif max_dim > 0.25:
            zoom_level = 11
        elif max_dim > 0.125:
            zoom_level = 12
        elif max_dim > 0.0625:
            zoom_level = 13
        else:
            zoom_level = 14

        # print(f"Zoom: {zoom_level}")
        return zoom_level

    def _mosaic_analytic_tiles(self, tiles_gdf, outfile=None):
        src_files_to_mosaic = []

        for _, tile in tiles_gdf.iterrows():
            # Open the satellite image file with rasterio
            # If gen_preview is TRUE then we are doing this for Previews and not Analytics.
            url = tile['analytic_url']
            src = rasterio.open(url)
            src_files_to_mosaic.append(src)

            outcome_id = tile['satl:outcome_id']
            product_version = tile['satl:product_version']
            if not self._is_version_valid(product_version):
                logger.warning(f"Tile Version Incompatible: {outcome_id}, ProdVer: {product_version}")

        # Create the mosaic
        mosaic, out_trans = merge(src_files_to_mosaic, indexes=[1, 2, 3])

        # Metadata for the mosaic
        meta = {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "crs": tiles_gdf.crs,
            "count": mosaic.shape[0],
            "dtype": mosaic.dtype
        }

        # Write the file out if a filename is provided.
        if outfile is not None:
            with rasterio.open(outfile, "w", **meta) as dest:
                dest.write(mosaic)
            logger.warning(f"Mosaic Complete:{outfile}")

        return mosaic, meta


    # def _georeference_preview(self, tile):
    #     with rasterio.open(tile['preview_url']) as src:

    #         min_x, min_y, max_x, max_y = shape(tile['geometry'].bounds)      
            
    #         pixel_size_x = (max_x - min_x) / src.width
    #         pixel_size_y = (max_y - min_y) / src.height
            
    #         transform = from_origin(min_x, max_y, pixel_size_x, pixel_size_y)
            
    #         # Create a MemoryFile for the output
    #         with MemoryFile() as memfile:
    #             with memfile.open(driver='GTiff', height=src.height, width=src.width,
    #                             count=src.count, dtype=src.dtypes[0], crs="epsg:4326", transform=transform) as dst:
    #                 dst.write(src.read())

    #             # After writing, open the MemoryFile for reading
    #             with memfile.open() as dst:
    #                 # You now have a dst that is a DatasetReader object containing the georeferenced data
    #                 # You can return this object, or perform additional operations on it as needed
    #                 return dst                
    #         # with rasterio.open(outpath, 'w', driver='GTiff', height=src.height, width=src.width,
    #         #                 count=src.count, dtype=src.dtypes[0], crs="epsg:4326", transform=transform) as dst:        
    #         #     dst.write(src.read())
    #     return None
    

    def _georeference_preview(self, tile):
        # Suppress the specific warning
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', NotGeoreferencedWarning)
            # Open the thumbnail and georeference it.
            with rasterio.open(tile['thumbnail_url']) as src:
                
                min_x, min_y, max_x, max_y = shape(tile['geometry']).bounds
                
                pixel_size_x = (max_x - min_x) / src.width
                pixel_size_y = (max_y - min_y) / src.height
                
                transform = from_origin(min_x, max_y, pixel_size_x, pixel_size_y)
                
                # Create a MemoryFile for the output
                with MemoryFile() as memfile:
                    with memfile.open(driver='GTiff', height=src.height, width=src.width,
                                    count=src.count, dtype=src.dtypes[0], crs="epsg:4326", transform=transform) as dst:
                        dst.write(src.read())

                    # After writing, open the MemoryFile for reading
                    return memfile.open()  # Return an open DatasetReader
    
    def _mosaic_preview_tiles(self, tiles_group, output_path=None)->Image:
        src_files_to_mosaic = []

        # Loop through each row in the GeoDataFrame and create a list of georeferenced previews.
        # Prepare for multi-threaded execution
        with ThreadPoolExecutor(max_workers=25) as executor:  # Adjust max_workers as needed
            # Start georeferencing and downsampling tasks
            future_to_tile = {executor.submit(self._georeference_preview, row): row for _, row in tiles_group.iterrows()}

            src_files_to_mosaic = []
            for future in as_completed(future_to_tile):
                try:
                    result = future.result()
                    src_files_to_mosaic.append(result)
                except Exception as e:
                    # Handle exceptions if any
                    print(f"Exception occurred: {e}")


        with warnings.catch_warnings():
            warnings.simplefilter('ignore', NotGeoreferencedWarning)
            mosaic_src, out_trans = merge(src_files_to_mosaic, indexes=[1, 2, 3])

        if output_path is not None:
            # Define the metadata for the mosaic
            out_meta = src_files_to_mosaic[0].meta.copy()
            out_meta.update({
                "driver": "PNG",
                "height": mosaic_src.shape[1],
                "width": mosaic_src.shape[2],
                "transform": out_trans,
                "count": 3
            })

            with warnings.catch_warnings():
                warnings.simplefilter('ignore', NotGeoreferencedWarning)
                with rasterio.open(output_path, "w", **out_meta) as dest:
                    dest.write(mosaic_src)

        return mosaic_src, out_trans

    def _is_version_valid(self, product_version):
        # Check that the version number is valid or not.
        return version.parse(product_version) >= version.parse(self.min_product_version)

    def _is_group_valid(self, group_df):
        global max_tile_count  # Use the global max_tile_count
        global invalid_outcome_ids
        tile_count = len(group_df)

        if max_tile_count is None:
            logger.error("Rejected: max_tile_count is not set!")
            return False

        capture_date = group_df.iloc[0]['capture_date']
        # Rejection based on tile coverage
        if tile_count < max_tile_count * self.min_tile_coverage_percent:
            logger.warning(f"Capture {capture_date} Rejected Due To Insufficient Tile Coverage: {tile_count}/{max_tile_count}")
            return False

        mean_cloud_cover = group_df['eo:cloud_cover'].mean()
        product_version = group_df.iloc[0]['satl:product_version']
        outcome_id = group_df.iloc[0]['satl:outcome_id']

        if not self._is_version_valid(product_version):
            invalid_outcome_ids.append(outcome_id)
            logger.warning(f"Capture Rejected Due To Version: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
            return False

        if pd.isna(mean_cloud_cover) or mean_cloud_cover > self.cloud_threshold:
            logger.warning(f"Capture Rejected Due To Cloud Cover: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
            return False

        return True

    def _ensure_dir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _get_max_dimensions_and_bounds(self, image_filenames):
        max_width, max_height = 0, 0
        largest_bounds = None

        for fname in image_filenames:
            with rasterio.open(fname) as src:
                width, height = src.width, src.height
                bounds = src.bounds

                max_width = max(max_width, width)
                max_height = max(max_height, height)

                if largest_bounds is None:
                    largest_bounds = bounds
                else:
                    largest_bounds = (
                        min(largest_bounds[0], bounds[0]),  # min left
                        min(largest_bounds[1], bounds[1]),  # min bottom
                        max(largest_bounds[2], bounds[2]),  # max right
                        max(largest_bounds[3], bounds[3])   # max top
                    )

        return max_width, max_height, largest_bounds

    def _resize_mosaics_to_largest(self, image_filenames):
        max_width, max_height, largest_bounds = self._get_max_dimensions_and_bounds(image_filenames)

        # Create a list to store the names of resized images
        new_filenames = []

        for fname in image_filenames:
            # Open the source file
            with rasterio.open(fname) as src:
                # Calculate new transform
                out_transform = rasterio.transform.from_bounds(*largest_bounds, max_width, max_height)

                # Update metadata
                out_meta = src.meta.copy()
                out_meta.update({
                    "height": max_height,
                    "width": max_width,
                    "transform": out_transform
                })

                # Create a destination array
                dest_data = np.zeros((src.count, max_height, max_width))

                reproject(
                    source=rasterio.band(src, range(1, src.count + 1)),  # All bands
                    destination=dest_data,
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=out_transform,
                    dst_crs=src.crs,
                    resampling=Resampling.cubic
                )

                # Change the filename for the new resized image
                new_fname = fname.replace(".tiff", "_resized.tiff")

                # Write out the resized image to a new file
                with rasterio.open(new_fname, 'w', **out_meta) as dest:
                    dest.write(dest_data)

                # Append the new filename to our list
                new_filenames.append(new_fname)

        return new_filenames

    def _create_animation_from_files(self, image_filenames, output_filename, pause_duration, bbox_aoi, font):
        logger.info(f"Creating Animation For Filenames: {image_filenames}")
        # Sort the image filenames based on the capture date
        image_filenames.sort(key=self._extract_date)

        resized_filenames = self._resize_mosaics_to_largest(image_filenames)

        # Create a writer object
        duration_ms = pause_duration * 1000  # Convert seconds to milliseconds

        # Seek the largest dimensions
        max_width, max_height, largest_bounds = self._get_max_dimensions_and_bounds(resized_filenames)

        writer = imageio.get_writer(output_filename, duration=duration_ms, macro_block_size=1, loop=0)

        # Iterate through the image filenames and add them to the animation
        for index, image_filename in enumerate(resized_filenames):
            logger.info(f"Animating File: {image_filename}")

            # Load the image using PIL
            image = Image.open(image_filename)
            resized_image = image.resize((max_width, max_height), Image.LANCZOS)
            # Now we use the largest dimensions for our blank canvas
            blank_image = Image.new('RGBA', (max_width, max_height), 'black')

            # blank_image.paste(resized_image.convert("RGBA"), offset)
            blank_image.paste(resized_image.convert("RGBA"), (0,0))

            # Create a drawing context
            draw = ImageDraw.Draw(blank_image)

            # Extract the date from the filename and format it
            date = self._extract_date(image_filename).strftime('%Y-%m-%dT%H%M%S')

            # Extract bounds from Polygon object
            minx, miny, maxx, maxy = bbox_aoi.bounds

            # Calculate center latitude and longitude
            center_lat = (miny + maxy) / 2
            center_long = (minx + maxx) / 2

            # Create the label text with Date and Lat/Long
            label_text = f"Date: {date} | Lat: {center_lat:.4f}, Long: {center_long:.4f}"

            # Calculate the width of the text
            text_width = draw.textlength(label_text, font=font)

            # Define the position to center the text horizontally, near the top vertically
            position = ((max_width - text_width) / 2, 10)

            # Draw the text on the image in yellow
            draw.text(position, label_text, fill="yellow", font=font)

            # Convert blank_image to a numpy array
            image_np = np.array(blank_image)

            writer.append_data(image_np)

        # Close the writer to finalize the animation
        writer.close()
        logger.info(f"Animation saved as: {output_filename}.")

    def _extract_date(self, filename):
        # Split the filename into its constituent parts
        parts = filename.split(os.sep)

        # Assume the date is in the second part of the filename and the first part of that second part
        date_str = parts[1].split('_')[1]

        # Convert the date string to a datetime object
        datetime_str = datetime.strptime(date_str, '%Y%m%dT%H%M%S')

        return datetime_str

