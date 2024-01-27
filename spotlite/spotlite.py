# Copyright (c) 2024 Satellogic USA Inc. All Rights Reserved.
#
# This file is the unified interface into the Spotlite Package and
# would be imported via: spotlight import Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.


from typing import Tuple, Dict, Optional, List, Type
from shapely.geometry import Polygon, Point, box
import webbrowser
from datetime import datetime
from pathlib import Path
import geopandas as gpd
from pandas.core.groupby import DataFrameGroupBy
from datetime import datetime, timedelta
import logging
from PIL import ImageFont

from .tile import TileManager
from .monitor import MonitorAgent
from .task import TaskingManager

logger = logging.getLogger(__name__)
tiles_gdf = None
        
class Spotlite:
    def __init__(self, key_id="", key_secret="", font_path=""):
        # Assigning default values to instance attributes
        self._ensure_logging_is_setup()
        self.key_id = key_id
        self.key_secret = key_secret
        self.font_path = font_path
        self._param = None  # Initialize _param for the property

        # Initialize the worker classes
        self.tile_manager = TileManager(self.key_id, self.key_secret)
        self.tasking_manager = TaskingManager(self.key_id, self.key_secret)
    
    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    # Main function to handle searching the archive.
    def create_tile_stack_animation(self, points: List[Dict[str, float]], width: float, start_date, end_date, save_and_animate=False):
        # For the list of points create a map with all of the points and bounding boxes on it.
        aois_list, points_list = self.tile_manager.create_aois_from_points(points, width)
        master_map = self.tile_manager.create_folium_map(points_list, aois_list)
        fig_obj = self.tile_manager.create_choropleth_map(aois_list)

        # Loop through the bbox aois and search and append the results to the map.
        logging.info(f"Number of AOIs: {len(aois_list)}.")
        
        animation_filename = None
        for index, aoi in enumerate(aois_list):
            logging.info(f"Processing AOI #: {index+1}")
            tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)

            if num_tiles >0:
                if 'eo:cloud_cover' not in tiles_gdf.columns:
                    logging.warning(f"Column 'eo:cloud_cover' doesn't exist for this AOI, skipping.")
                    continue
                else:

                    # We want to save the tiles into their respective captures to then animate them.
                    logging.warning(f"Found Total Captures: {num_captures}, Total Tiles: {num_tiles}.")

                    # get the font with local helper function because it depends on config
                    font = self._get_font()
                    # Save and animate the image capture tiles
                    if save_and_animate == 'y':
                        # Save and animate the tiles
                        result = self.tile_manager.animate_tile_stack(tiles_gdf, aoi, font)

                        # Check for valid result before proceeding
                        if result:
                            animation_filename, fnames = result
                        else:
                            animation_filename = None
                            logging.warning("Animation not created. Skipping...")
                            continue

                    master_map = self.tile_manager.update_map_with_tiles(master_map, tiles_gdf, animation_filename, aoi)

        if master_map:
            now = datetime.now()
            master_map_filename = f"maps/Search_Results_Map_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
            master_map.save(master_map_filename)  # Save the master_map to a file
            # webbrowser.open(master_map_filename)  # Open the saved file in the browser

    def save_footprints(self, aoi: Polygon, start_date_str: str, end_date_str: str, out_filename=None) -> bool:
        
        # Function to split the date range into chunks
        chunk_size_days = 90

        date_chunks = list(self._date_range_chunks(start_date_str, end_date_str, chunk_size_days))

        for chunk_start, chunk_end in date_chunks:
            chunk_start_str = chunk_start.split('T')[0]  # Split by 'T' and take the first part (date)
            chunk_end_str = chunk_end.split('T')[0]
            logging.warning(f"Date Range For Search: {chunk_start_str} - {chunk_end_str}")
            tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, chunk_start_str, chunk_end_str)

            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            # If no tiles found then return True.
            if num_tiles == 0:
                return True  
            
            grouped = self.tile_manager.group_by_outcome_id(tiles_gdf)
            # Create a new GeoDataFrame to store results
            output_gdf = gpd.GeoDataFrame(columns=['outcome_id', 'cloud_cover_mean', 'capture_date', 'geometry'])

            rows_list = []

            for outcome_id, group in grouped:
                cloud_cover_mean = int(round(group['eo:cloud_cover'].mean()))
                combined_footprint = group.geometry.unary_union
                capture_date = group.iloc[0]['capture_date']

                # Add the information to the new GeoDataFrame
                rows_list.append({
                    'outcome_id': outcome_id, 
                    'cloud_cover_mean': cloud_cover_mean, 
                    'capture_date': capture_date, 
                    'geometry': combined_footprint
                })
            
            output_gdf = gpd.GeoDataFrame(rows_list)

            now = datetime.now()

            out_filename = f"maps/Footprints_{chunk_start_str}-{chunk_end_str}_Created-{now.strftime('%Y-%m-%d_%H-%M-%S')}.geojson"
            
            try:
                output_gdf.to_file(out_filename, driver='GeoJSON')
                logger.warning(f"Footprint File Saved: {out_filename}")
            except Exception as e:  # Correct syntax and catch general exception
                logging.error(f"Failed to write footprint file: {e}")
                return False

        
        return True

    def monitor_subscriptions_for_captures(self, period_int=None, subscriptions_file_path_str=None):
        """Start The Subscription Monitor - searches AOI for new captures in the past period
        and sends an email to a defined list of people."""

        # Mechanism of timing of monitoring runs has changed, the period is in the subscriptions.geojson.
        period_int = None
        self.monitor = MonitorAgent(self.key_id, self.key_secret)

        # Start the Monitor
        try:
            self.monitor.run()

        except AttributeError as e:  
            logger.error("Monitoring Agent Failed: '%s'", str(e))  

    def create_cloud_free_basemap(self, aoi: Polygon, start_date: str, end_date: str):
        """Create a cloud free basemap using the latest cloud free tile from the archive."""
        # Search The Archive
        tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)

        logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")

        if num_tiles > 0:
            latest_cloud_free_tiles = self.tile_manager.filter_and_sort_tiles(tiles_gdf)

            folium_basemap = self.tile_manager.create_folium_basemap(latest_cloud_free_tiles)

            now = datetime.now().strftime("%Y-%m-%dT%H%M%SZ")
            filename = f'maps/Basemap_{now}.html'
            folium_basemap.save(filename)
            webbrowser.open(filename)

            logging.warning(f"Basemap Created With Latest Cloud Free Tiles.")
        else:
            logging.warning("No tiles found!")

    def create_age_heatmap(self, aoi, start_date, end_date, out_filename=None):
        tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)

        logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
        if num_tiles > 0:
            # Sort by age so that youngest tiles are last (and thus displayed on top)
            hmap = self.tile_manager.age_heatmap(tiles_gdf, out_filename)

            logging.warning("Heat Map Complete: maps folder...")
        else:
            logging.warning("No tiles found!")            

    def create_cloud_heatmap(self, aoi, start_date, end_date, out_filename=None):
        tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)
        logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")

        if num_tiles > 0:
            fig_obj = self.tile_manager.cloud_heatmap(tiles_gdf, out_filename)

            logging.warning("Heat Map Complete And Saved To Maps Folder...")
        else:
            logging.warning("No Tiles Found.")

    def create_count_heatmap(self, aoi, start_date, end_date, out_filename=None):
        tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)

        logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
        if num_tiles > 0:
            # Create the heatmap for the data.
            hmap = self.tile_manager.count_heatmap(tiles_gdf, out_filename)

            logging.warning(f"Heat Map Complete with Num_Tiles: {num_tiles}, Num_Captures: {num_captures}")
        else:
            logging.warning("No tiles found!")

    def download_image(self, outcome_id: str, output_dir: str):
        tiles_gdf = self.tile_manager.get_tiles_for_outcome_id(outcome_id)
        self.tile_manager.download_tiles(tiles_gdf, output_dir) 

    def download_tiles(self, points: List[Dict[str, float]], width: float, start_date, end_date, output_dir=None):
        """"Downloads Tiles for a specified list of points with a width during a time period"""
        # Create aois_list
        aois_list, points_list = self.tile_manager.create_aois_from_points(points, width)

        # Loop through the bbox aois and search and append the results to the map.
        logging.info(f"Number of AOIs Entered: {len(aois_list)}.")

        for index, aoi in enumerate(aois_list):
            logging.info(f"Processing AOI #: {index+1}")
            tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi, start_date, end_date)

            if num_tiles >0:
                # We want to save the tiles into their respective captures to then animate them.
                logging.warning(f"Total Captures: {num_captures}, Total Tiles: {num_tiles}.")

                # Save tiles into a directory for this job with.
                self.tile_manager.download_tiles(tiles_gdf, output_dir) # tiles_gdf is stored inside searcher after search.

        logging.warning("Tile Download Complete!")

    # Function to split the date range into two-week chunks
    def _date_range_chunks(self, start_date: str, end_date: str, chunk_size_days=14):
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        delta = timedelta(days=chunk_size_days)  # default two weeks

        while start < end:
            chunk_end = min(start + delta, end)
            yield start.isoformat(), chunk_end.isoformat()
            start = chunk_end

    def _get_font(self) -> ImageFont:
        """Returns either a specified font or falls back to the default font."""
        try:
            font_path = self.font_path

            # if path exists
            if Path(font_path).is_file():
                logging.info("using custom font: '%s'", font_path)
                enlarged_font_size = 100
                font = ImageFont.truetype(font_path, enlarged_font_size)

            else:
                logging.info("specified font path not found: '%s'", font_path)
                raise AttributeError

        except AttributeError:
            logging.info("using default font.")
            font = ImageFont.load_default()

        return font
        
    def _ensure_logging_is_setup(self):
        """Ensure that logging is set up."""
        if not logging.root.handlers:  # Check if any handlers are already set up
            self.setup_logging()

    def _setup_logging(self):
        """Set up logging for the package."""
        now = datetime.now().strftime("%d-%m-%YT%H%M%S")
        logging.basicConfig(filename=f"log/UserApp-{now}.txt", level=logging.DEBUG, 
                            format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        logging.getLogger().addHandler(console)