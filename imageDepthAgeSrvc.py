# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

import schedule
import time
from mapUtils import create_heatmap_for_age, create_heatmap_for_image_count
from satellogicUtils import search_archive
import config
from datetime import datetime
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)

FROM_EMAIL = config.EMAIL_ADDRESS
PERIOD = config.SUBC_MON_FREQUENCY # In Minutes
POLYGON_FILES_PATH = config.POLYGON_FILES_PATH

def update_maps():
    global POLYGON_FILES_PATH
    # 1. Load the polygon file with multiple polygons
    gdf_polygons = gpd.read_file(POLYGON_FILES_PATH)
    
    start_date_str = "2021-01-01"
    end_date_str = datetime.utcnow().strftime("%Y-%m-%d")

    # 2. For each polygon feature extract the polygon and pass it into the following functions
    for idx, row in gdf_polygons.iterrows():
        country = row['country']  # Assuming 'country' is a column in your GeoJSON
        aoi = row['geometry']
        
        logger.warning(f"Checking Region: {country}.")
        
        # Search for images within this AOI
        tiles_gdf, num_tiles, num_captures = search_archive(aoi, start_date_str, end_date_str)
        
        if num_tiles > 0:
            logging.warning(f"Num_Captures: {num_captures}, Num_Tiles: {num_tiles}.")
            tiles_gdf.sort_values(by='data_age', ascending=False, inplace=True)
            # Create heatmaps
            if not tiles_gdf.empty:
                hmap_age = create_heatmap_for_age(tiles_gdf)
                hmap_count = create_heatmap_for_image_count(tiles_gdf)

    logger.warning(f"Created Heatmaps For This Period, Searching Again In {config.MAP_UPDATE_FREQUENCY} Minutes.")

    return True

def main():
    now = datetime.now().strftime("%Y-%m-%dT%H%M%S")
    logging.basicConfig(filename=f"log/ImageDepthAgeSrvc-{now}.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Add StreamHandler to log to console as well
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.getLogger().addHandler(console)

    update_maps()
    schedule.every(config.MAP_UPDATE_FREQUENCY).minutes.do(update_maps)

    while True:
        schedule.run_pending()
        time.sleep(1)

        # schedule.every().day.at(query_time_utc).do(task)



if __name__ == "__main__":
    main()


