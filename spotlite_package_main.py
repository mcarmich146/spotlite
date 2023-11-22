# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

import datetime
from satellogicUtils import get_lat_long_from_place, ensure_dir
from datetime import datetime
from dateutil.relativedelta import relativedelta
import webbrowser
import tkinter as tk
from tkinter import filedialog
import geopandas as gpd
import logging
from spotlite import Searcher, Visualizer, TaskingManager
import config

def main():
    # Make Sure All The Directories Are Present
    ensure_dir("log")
    ensure_dir("images")
    ensure_dir("maps")
    ensure_dir("invalid_outcome_ids")
    ensure_dir("search_results")
    ensure_dir("points_to_monitor")

    # Setup Logging
    now = datetime.now().strftime("%d-%m-%YT%H%M%S")
    logging.basicConfig(filename=f"log/UserApp-{now}.txt", level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    # Add StreamHandler to log to console as well
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.getLogger().addHandler(console)

    place = ""

    searcher = Searcher()
    searcher.key_id = config.KEY_ID
    searcher.key_secret = config.KEY_SECRET
    visualizer = Visualizer()
    tasker = TaskingManager(config.KEY_ID, config.KEY_SECRET)

    while True:
        print("Options:")
        print("1. Search And Animate Site.")
        print("2. Search And Plot Images With Thumbnails.")
        print("3. Create Cloud Free Basemap.")
        print("4. Create Heat Map Of Collection Age.")
        print("5. Create Heatmap Of Imagery Depth.")
        print("6. Create Heat Map Of Cloud Cover For Area.")
        print("7. Deleted - Subscriptions.")
        print("8. Enter New Tasking.")
        print("9. Download Tiles For BBox.")
        print("q. For Quit...")
        
        user_choice = input("Enter your choice: ")
        if user_choice == '1':
            use_geojson = input("Do you have a geojson POINT file (y/n)?: ").lower()
            
            if use_geojson == 'y':
                # Open the file dialog to select the GeoJSON file
                root = tk.Tk()
                root.withdraw()
                geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                            filetypes=[("GeoJSON files", "*.geojson")])
                if geojson_filepath:
                    logging.info(f"GeoJSON file selected: {geojson_filepath}")
                    tiles_gdf = gpd.read_file(geojson_filepath)
                    points = [{'lat': row.geometry.y, 'lon': row.geometry.x} for index, row in tiles_gdf.iterrows()]
                else:
                    logging.warning("No file selected. Please try again.")
                    # Optionally, add logic to re-prompt the user or handle this situation
                    break
            else:
                place = input(f"Enter the place name or lat,lon in dec. deg.: ")
                lat, lon = get_lat_long_from_place(place)
                points = [{'lat': lat, 'lon': lon}]
            
            # Set the Bbox width
            width = float(input("Provide search box width (km):"))
            
            # Get the current date and calculate the date one month prior
            now = datetime.now()
            one_month_ago = now - relativedelta(months=1) 

            # Format the dates to string (YYYY-MM-DD)
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')

            start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.info(f"Date Range For Search: {start_date} - {end_date}")
            
            # For the list of points create a map with all of the points and bounding boxes on it.
            aois_list = searcher.create_aois_from_points(points, width)
            master_map = visualizer.create_folium_map(points, aois_list)
            fig_obj = visualizer.create_choropleth_map(aois_list)
            
            # Loop through the bbox aois and search and append the results to the map.
            logging.info(f"Number of AOIs: {len(aois_list)}.")
            save_and_animate = input("Save and Animate (y/n)?: ").lower() or "y" # apply this to every aoi.
            animation_filename = ""
            for index, aoi in enumerate(aois_list):
                logging.info(f"Processing AOI #: {index+1}")
                tiles_gdf, num_tiles, num_captures = searcher.search_archive(aoi, start_date, end_date)
                                                
                if num_tiles >0:
                    if 'eo:cloud_cover' not in tiles_gdf.columns:
                        logging.warning(f"Column 'eo:cloud_cover' doesn't exist for this AOI, skipping.")
                        continue
                    else:                                    
                        
                        # We want to save the tiles into their respective captures to then animate them.
                        logging.warning(f"Found Total Captures: {num_captures}, Total Tiles: {num_tiles}.")
                        # Save and animate the image capture tiles
                        if save_and_animate == 'y':
                            # Save and animate the tiles
                            result = visualizer.animate_tile_stack(tiles_gdf, aoi)

                            # Check for valid result before proceeding
                            if result:
                                animation_filename, fnames = result
                            else:
                                animation_filename = ""
                                logging.warning("Animation not created. Skipping...")
                                continue
                        
                        master_map = visualizer.update_map_with_tiles(master_map, tiles_gdf, animation_filename, aoi)
                        fig_obj = visualizer.cloud_heatmap(tiles_gdf, fig_obj)

            if master_map:
                master_map_filename = f"maps/Search_Results_Map_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
                master_map.save(master_map_filename)  # Save the master_map to a file
                # webbrowser.open(master_map_filename)  # Open the saved file in the browser  
                
        elif user_choice == '2': # Search and Plot Images With Thumbnail.
            logging.warning("Searching and Plotting Images With Browse Images.")
            # Open the file dialog to select the GeoJSON file
            logging.warning("Provide geojson polygon file.")
            root = tk.Tk()
            root.withdraw()
            geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                        filetypes=[("GeoJSON files", "*.geojson")])
            if geojson_filepath:
                logging.info(f"GeoJSON polygon file selected: {geojson_filepath}")
                input_gdf = gpd.read_file(geojson_filepath)
                heatmap_aoi = input_gdf.iloc[0].geometry.__geo_interface__
            else:
                logging.warning("No geojson file!")
                break
            
            # Format the dates to string (YYYY-MM-DD)
            # Get the current date and calculate the date one month prior
            now = datetime.utcnow()
            one_month_ago = now - relativedelta(months=1) 
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
            search_start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            search_end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.warning(f"Date Range For Search: {search_start_date} - {search_end_date}")

            # Search The Archive
            tiles_gdf, num_tiles, num_captures = searcher.search_archive(heatmap_aoi, search_start_date, search_end_date)
            
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            if num_tiles > 0:                
                # Sort by capture date
                tiles_gdf.sort_values('capture_date', ascending=False, inplace=True)
            
                folium_basemap = visualizer.create_folium_basemap(tiles_gdf)
         
                now = datetime.now().strftime("%Y-%m-%dT%H%M%SZ")
                filename = f'maps/Basemap_{now}.html'
                folium_basemap.save(filename)

                logging.warning(f"Basemap Created With Latest Cloud Free Tiles And Saved To Maps Folder.")
            else:
                logging.warning("No tiles found!")
                continue
            
            continue
        elif user_choice == '8': # Manage Taskings.
            while True:
                print("Manage Taskings:")
                print("1. Create New Tasking Via API") # Validated
                print("2. Check Status of Task") # Validated
                print("3. Cancel Task") # Validated
                print("4. Query and Download Image") #Validated
                print("5. Check Client Config.") # Validated
                print("6. Search Products By Status.") # Validated
                print("7. Check Available Product List.") #Validated
                print("q. Back to main menu.")
                sub_choice = input("Enter your choice: ")

                if sub_choice == '1':  # Create new tasking via API
                    tasking_df = tasker.create_new_tasking()
                    print(f"Tasking Result: {tasking_df}")
                    continue
                elif sub_choice == '2': # Check status of task
                    task_id = input("Specify Task Id: ")
                    print(f"Status: {tasker.check_task_status(task_id)}")
                    continue
                elif sub_choice == '3': # cancel_task
                    task_id = input("Specify Task Id: ")
                    print(f"Status: {tasker.cancel_task(task_id)}")
                    continue
                elif sub_choice == '4': # query_and_download_image
                    scene_set_id = input("SceneSetID?: ")
                    download_dir = input("Target Relative Download Directory? (images):") or "images"
                    print(f"Downloaded Image Filename: {tasker.query_and_download_image(scene_set_id, download_dir)}")
                    continue
                elif sub_choice == '5': # Check Client Config
                    print(f"Client Config: {tasker.check_account_config()}")
                elif sub_choice == '6': # Search products by status.
                    df = tasker.query_tasking_products_by_status("completed")
                    print(f"Completed Products: {df}")
                elif sub_choice == '7': # Check available products list.
                    df = tasker.query_available_tasking_products()
                    print(f"Availble Products: \n{df}")
                elif sub_choice == 'q': # Return to main menu
                    break
                else:
                    print("Invalid Choice.")
                    continue
        elif user_choice == '4': # Create heatmap of imagery age.
            # Open the file dialog to select the GeoJSON file
            print("Provide geojson polygon file.")
            root = tk.Tk()
            root.withdraw()
            geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                        filetypes=[("GeoJSON files", "*.geojson")])
            if geojson_filepath:
                logging.info(f"GeoJSON file selected: {geojson_filepath}")
                tiles_gdf = gpd.read_file(geojson_filepath)
                heatmap_aoi = tiles_gdf.iloc[0].geometry.__geo_interface__
            else:
                logging.warning("No geojson file!")
                break
            
            # Format the dates to string (YYYY-MM-DD)
            # Get the current date and calculate the date one month prior
            now = datetime.utcnow()
            one_month_ago = now - relativedelta(months=1) 
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
            search_start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            search_end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.warning(f"Date Range For Search: {search_start_date} - {search_end_date}")

            tiles_gdf, num_tiles, num_captures = searcher.search_archive(heatmap_aoi, search_start_date, search_end_date)

            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            if num_tiles > 0:
                # Sort by age so that youngest tiles are last (and thus displayed on top)
                hmap = visualizer.age_heatmap(tiles_gdf, out_filename=None)
            
                logging.warning("Heat Map Complete: maps folder...")
            else:
                logging.warning("No tiles found!")
                continue
        
        elif user_choice == '9': # Download Tiles For BBox
            place = input(f"Enter the place name or lat,lon in dec. deg.: ")
            lat, lon = get_lat_long_from_place(place)
            points = [{'lat': lat, 'lon': lon}]
            
            # Set the Bbox width
            width = float(input("Provide search box width (km):"))
            
            # Get the current date and calculate the date one month prior
            now = datetime.now()
            one_month_ago = now - relativedelta(months=1) 

            # Format the dates to string (YYYY-MM-DD)
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')

            start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.info(f"Date Range For Search: {start_date} - {end_date}")
            
            # Create aois_list
            aois_list = searcher.create_aois_from_points(points, width)
            # Create master map(s)
            master_map = visualizer.create_folium_map(aois_list)

            # For the list of points create a map with all of the points and bounding boxes on it.
            # master_map, aois_list = visualizer.process_multiple_points_to_bboxs(points, width)
            
            # Loop through the bbox aois and search and append the results to the map.
            logging.info(f"Number of AOIs Entered: {len(aois_list)}.")
            
            for index, aoi in enumerate(aois_list):
                logging.info(f"Processing AOI #: {index+1}")
                tiles_gdf, num_tiles, num_captures = searcher.search_archive(aoi, start_date, end_date)
                               
                if num_tiles >0:
                    # We want to save the tiles into their respective captures to then animate them.
                    logging.warning(f"Total Captures: {num_captures}, Total Tiles: {num_tiles}.")
                    
                    # Save tiles into a directory for this job with.
                    searcher.save_tiles(aoi) # tiles_gdf is stored inside searcher after search.

            logging.warning("Tile Download Complete!")
            continue
        elif user_choice == '5': # Create Heatmap for Stack Depth
            logging.warning("Create Heatmap Of Depth Of Stack.")
            # Open the file dialog to select the GeoJSON file
            logging.warning("Provide geojson polygon file.")
            root = tk.Tk()
            root.withdraw()
            geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                        filetypes=[("GeoJSON files", "*.geojson")])
            if geojson_filepath:
                logging.info(f"GeoJSON file selected: {geojson_filepath}")
                tiles_gdf = gpd.read_file(geojson_filepath)
                heatmap_aoi = tiles_gdf.iloc[0].geometry.__geo_interface__
            else:
                logging.warning("No geojson file!")
                break
            
            # Format the dates to string (YYYY-MM-DD)
            # Get the current date and calculate the date one month prior
            now = datetime.utcnow()
            one_month_ago = now - relativedelta(months=1) 
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
            search_start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            search_end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.warning(f"Date Range For Search: {search_start_date} - {search_end_date}")

            tiles_gdf, num_tiles, num_captures = searcher.search_archive(heatmap_aoi, search_start_date, search_end_date)
 
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            if num_tiles > 0:
                # Create the heatmap for the data.
                hmap = visualizer.count_heatmap(tiles_gdf)
            
                logging.warning(f"Heat Map Complete with Num_Tiles: {num_tiles}, Num_Captures: {num_captures}")
            else:
                logging.warning("No tiles found!")
                continue
            
        elif user_choice == '3': # Create Cloud Free Tile Basemap - Works but seem like non-sense?
            logging.warning("Create Cloud Free Tile Basemap.")
            # Open the file dialog to select the GeoJSON file
            logging.warning("Provide geojson polygon file.")
            root = tk.Tk()
            root.withdraw()
            geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                        filetypes=[("GeoJSON files", "*.geojson")])
            if geojson_filepath:
                logging.info(f"GeoJSON file selected: {geojson_filepath}")
                tiles_gdf = gpd.read_file(geojson_filepath)
                heatmap_aoi = tiles_gdf.iloc[0].geometry.__geo_interface__
            else:
                logging.warning("No geojson file!")
                break
            
            # Format the dates to string (YYYY-MM-DD)
            # Get the current date and calculate the date one month prior
            now = datetime.utcnow()
            one_month_ago = now - relativedelta(months=1) 
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
            search_start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            search_end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.warning(f"Date Range For Search: {search_start_date} - {search_end_date}")

            # Search The Archive
            tiles_gdf, num_tiles, num_captures = searcher.search_archive(heatmap_aoi, search_start_date, search_end_date)
            
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            if num_tiles > 0:
                latest_cloud_free_tiles = visualizer.filter_and_sort_tiles(tiles_gdf)
            
                folium_basemap = visualizer.create_folium_basemap(latest_cloud_free_tiles)
         
                now = datetime.now().strftime("%Y-%m-%dT%H%M%SZ")
                filename = f'maps/Basemap_{now}.html'
                folium_basemap.save(filename)
                webbrowser.open(filename)

                logging.warning(f"Basemap Created With Latest Cloud Free Tiles.")
            else:
                logging.warning("No tiles found!")
                continue
            
            continue
        elif user_choice == '6': # Create for heat map for cloud cover for latest tiles.
            # Open the file dialog to select the GeoJSON file
            print("Provide geojson polygon file.")
            root = tk.Tk()
            root.withdraw()
            geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
                                                        filetypes=[("GeoJSON files", "*.geojson")])
            if geojson_filepath:
                logging.info(f"GeoJSON file selected: {geojson_filepath}")
                input_gdf = gpd.read_file(geojson_filepath)
                heatmap_aoi = input_gdf.iloc[0].geometry.__geo_interface__
            else:
                logging.warning("No geojson file!")
                break
            
            # Format the dates to string (YYYY-MM-DD)
            # Get the current date and calculate the date one month prior
            now = datetime.utcnow()
            one_month_ago = now - relativedelta(months=1) 
            end_date_str = now.strftime('%Y-%m-%d')
            one_month_ago_str = one_month_ago.strftime('%Y-%m-%d')
            search_start_date = input("Enter start date (YYYY-MM-DD) or press enter for 1 month ago: ") or one_month_ago_str
            search_end_date = input("Enter end date (YYYY-MM-DD) or press enter for now: ") or end_date_str
            logging.warning(f"Date Range For Search: {search_start_date} - {search_end_date}")

            tiles_gdf, num_tiles, num_captures = searcher.search_archive(heatmap_aoi, search_start_date, search_end_date)
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")

            if num_tiles > 0:
                fig_obj = visualizer.cloud_heatmap(tiles_gdf)
                if fig_obj:
                    fig_filename = f"maps/CloudCover_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
                    fig_obj.write_html(fig_filename)
                logging.warning("Heat Map Complete And Saved To Maps Folder...")
            else:
                logging.warning("No tiles found!")
                continue
        elif user_choice == 'q': # Q for quit
            print("Exiting. Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")
            continue

if __name__ == "__main__":
    main()

