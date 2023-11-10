import pickle
import datetime
import config
from satellogicUtils import get_lat_long_from_place, search_archive, save_and_animate_tiles, print_invalid_outcome_ids, save_tiles_for_bbox, create_cloud_free_basemap, setup_GDF, group_items_into_GPDF, ensure_dir
from mapUtils import create_bounding_box, update_map_with_tiles, process_multiple_points_to_bboxs, create_heatmap_for_age, create_heatmap_for_cloud, process_multiple_points_choropleth, create_map, create_heatmap_for_image_count, create_folium_basemap
from subscriptionUtils import add_subscription, list_subscriptions, delete_subscription
from satellogicTaskingAPI import create_new_tasking, check_task_status, cancel_task, query_and_download_image, check_account_config, query_tasking_products_by_status, query_available_tasking_products
from datetime import datetime
from dateutil.relativedelta import relativedelta
import webbrowser
import tkinter as tk
from tkinter import filedialog
import geopandas as gpd
import logging

defaultUserEmail = config.EMAIL_ADDRESS

def main():
    # Make Sure All The Directories Are Present
    ensure_dir("log")
    ensure_dir("images")
    ensure_dir("maps")
    ensure_dir("invalid_outcome_ids")
    ensure_dir("search_results")

    # Setup Logging
    now = datetime.now().strftime("%d-%m-%YT%H%M%S")
    logging.basicConfig(filename=f"log/UserApp-{now}.txt", level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    # Add StreamHandler to log to console as well
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.getLogger().addHandler(console)

    place = ""
    user_email = ""
    while True:
        # Life Cycle Steps We Are Emulating:
        #     1) User enters place name or lat/long for search
        #     2) Search the archive for tiles and visualize them.
        #     3) Access the full resolution Rapid Response products
        #     4) Animate tiles time sequence for context and change monitoring
        #     5) Order different product formats 
        #     6) Create new subscription areas to monitor for images coming in
        #     7) Analyze tiles to extract analytics/information/intelligence
        #     8) Create new tasking activities for high priority POIs
        #     9) Repeat

        print("Options:")
        print("1. Search And Animate Site")
        print("2. Search And Plot Images With Thumbnails.")
        print("3. Create Cloud Free Basemap.")
        print("4. Create Heat Map Of Collection Age.")
        print("5. Create Heatmap Of Imagery Depth.")
        print("6. Download Tiles For BBox")
        print("7. Manage Subscriptions")
        print("8. Enter New Tasking")
        print("9. Create Heat Map Of Cloud Cover For Area.")
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
            master_map, aois_list = process_multiple_points_to_bboxs(points, width)
            fig_obj, aois_list_chor = process_multiple_points_choropleth(points, width)
            
            # Loop through the bbox aois and search and append the results to the map.
            logging.info(f"Number of AOIs: {len(aois_list)}.")
            save_and_animate = input("Save and Animate (y/n)?: ").lower() or "y" # apply this to every aoi.
            animation_filename = ""
            for index, aoi in enumerate(aois_list):
                logging.info(f"Processing AOI #: {index+1}")
                tiles_gdf, num_tiles, num_captures = search_archive(aoi, start_date, end_date)
                                                
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
                            result = save_and_animate_tiles(tiles_gdf, aoi)

                            # Check for valid result before proceeding
                            if result:
                                animation_filename, fnames = result
                            else:
                                animation_filename = ""
                                logging.warning("Animation not created. Skipping...")
                                continue
                        
                        master_map = update_map_with_tiles(master_map, tiles_gdf, animation_filename, aoi)
                        fig_obj = create_heatmap_for_cloud(tiles_gdf, fig_obj)

            if fig_obj:
                master_map_filename = f"maps/Search_Results_Map_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
                master_map.save(master_map_filename)  # Save the master_map to a file
                webbrowser.open(master_map_filename)  # Open the saved file in the browser  
                
                fig_filename = f"maps/Choropleth_Map_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
                # write_html(fig_obj, fig_filename)
                fig_obj.write_html(fig_filename)
                fig_obj.show() 
            print_invalid_outcome_ids()
            
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
            tiles_gdf, num_tiles, num_captures = search_archive(heatmap_aoi, search_start_date, search_end_date)
            
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            if num_tiles > 0:                
                # Sort by capture date
                tiles_gdf.sort_values('capture_date', ascending=False, inplace=True)
            
                m = create_folium_basemap(tiles_gdf)
         
                now = datetime.now().strftime("%Y-%m-%dT%H%M%SZ")
                filename = f'maps/Basemap_{now}.html'
                m.save(filename)
                # webbrowser.open(filename)

                logging.warning(f"Basemap Created With Latest Cloud Free Tiles And Saved To Maps Folder.")
            else:
                logging.warning("No tiles found!")
                continue
            
            continue
        elif user_choice == '7':  # Manage Subscriptions.
            while True:
                print("Subscription Options:")
                print("1. List all subscriptions")
                print("2. Add a subscription")
                print("3. Delete a subscription")
                print("q. Back to main menu.")
                sub_choice = input("Enter your choice: ")

                if sub_choice == '1':  # List Subscriptions
                    list_subscriptions()
                elif sub_choice == '2':  # Add Subscription
                    place_location = input("Subscription Location: Enter place name or lat, long:")
                    lat, lon = get_lat_long_from_place(place_location)
                    if all(char.isdigit() or char in ',.- ' for char in place_location):  
                        subscription_name = input("Enter a name for this subscription:")
                    else:
                        subscription_name = place_location
                    print(f"Lat/Lon: {lat},{lon}")
                    bbox = create_bounding_box(lat, lon, float(input("Enter Bbox Width(km):")))

                    map_file = create_map(lat, lon, bbox)
                    user_emails_input = input("Provide User Email(s), separate emails w ',': ") or defaultUserEmail  # Make sure defaultUserEmail is defined
                    user_emails = [email.strip() for email in user_emails_input.split(',')]
                    add_subscription(user_emails, subscription_name, bbox)
                    list_subscriptions()
                elif sub_choice == '3':  # Delete Subscription
                    list_subscriptions()
                    sub_id = input("Enter the ID of the subscription you want to delete: ")
                    delete_successful = delete_subscription(sub_id)  # Assume this function returns a boolean
                    if delete_successful:
                        logging.info(f"Subscription {sub_id} deleted.")
                    list_subscriptions()
                elif sub_choice == 'q':  # Return To Main Menu
                    break
                else:
                    logging.warning("Invalid choice. Please try again.")

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
                    tasking_df = create_new_tasking()
                    print(f"Tasking Result: {tasking_df}")
                    continue
                elif sub_choice == '2': # Check status of task
                    task_id = input("Specify Task Id: ")
                    print(f"Status: {check_task_status(task_id)}")
                    continue
                elif sub_choice == '3': # cancel_task
                    task_id = input("Specify Task Id: ")
                    print(f"Status: {cancel_task(task_id)}")
                    continue
                elif sub_choice == '4': # query_and_download_image
                    scene_set_id = input("SceneSetID?: ")
                    download_dir = input("Target Relative Download Directory? (images):") or "images"
                    print(f"Downloaded Image Filename: {query_and_download_image(scene_set_id, download_dir)}")
                    continue
                elif sub_choice == '5': # Check Client Config
                    print(f"Client Config: {check_account_config()}")
                elif sub_choice == '6': # Search products by status.
                    df = query_tasking_products_by_status("completed")
                    print(f"Completed Products: {df}")
                elif sub_choice == '7': # Check available products list.
                    df = query_available_tasking_products()
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

            # aggregation_size_km = input("Set Heat Map Aggregation Size (km):")  # get this value from the user
            
            # Define the filepath for the pickle file
            pickle_filepath = 'search_results\items_cache.pkl'
            
            # # Check if the pickle file exists
            # if os.path.exists(pickle_filepath):
            #     # Load items from the pickle file
            #     with open(pickle_filepath, 'rb') as file:
            #         items, num_tiles, num_captures = pickle.load(file)
            #     logging.warning(f"Pickle File Loaded! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            # else:
            # Fetch data and save to a pickle file
            tiles_gdf, num_tiles, num_captures = search_archive(heatmap_aoi, search_start_date, search_end_date)
            with open(pickle_filepath, 'wb') as file:
                pickle.dump((tiles_gdf, num_tiles, num_captures), file)
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            if num_tiles > 0:
                # Sort by age so that youngest tiles are last (and thus displayed on top)
                tiles_gdf.sort_values(by='data_age', ascending=False, inplace=True)
                hmap = create_heatmap_for_age(tiles_gdf)
            
                logging.warning("Heat Map Complete: maps folder...")
            else:
                logging.warning("No tiles found!")
                continue
        
        elif user_choice == '6': # Download Tiles For BBox
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
            master_map, aois_list = process_multiple_points_to_bboxs(points, width)
            
            # Loop through the bbox aois and search and append the results to the map.
            logging.info(f"Number of AOIs Entered: {len(aois_list)}.")
            
            for index, aoi in enumerate(aois_list):
                logging.info(f"Processing AOI #: {index+1}")
                tiles_gdf, num_tiles, num_captures = search_archive(aoi, start_date, end_date)
                               
                if num_tiles >0:
                    # We want to save the tiles into their respective captures to then animate them.
                    logging.warning(f"Total Captures: {num_captures}, Total Tiles: {num_tiles}.")
                    
                    # Save tiles into a directory for this job with.
                    save_tiles_for_bbox(tiles_gdf, aoi)

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

            # aggregation_size_km = input("Set Heat Map Aggregation Size (km):")  # get this value from the user
            
            # Define the filepath for the pickle file
            # pickle_filepath = 'search_results\depth_tiles_cache.pkl'
            
            # # Check if the pickle file exists
            # if os.path.exists(pickle_filepath):
            #     # Load items from the pickle file
            #     with open(pickle_filepath, 'rb') as file:
            #         items, num_tiles, num_captures = pickle.load(file)
            #     logging.warning(f"Pickle File Loaded! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            # else:
            # Fetch data and save to a pickle file
            tiles_gdf, num_tiles, num_captures = search_archive(heatmap_aoi, search_start_date, search_end_date)
            # with open(pickle_filepath, 'wb') as file:
            #     pickle.dump((tiles_gdf, num_tiles, num_captures), file)
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            if num_tiles > 0:
                # Sort by age so that youngest tiles are last (and thus displayed on top)
                tiles_gdf.sort_values(by='data_age', ascending=False, inplace=True)
                hmap = create_heatmap_for_image_count(tiles_gdf)
            
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
            tiles_gdf, num_tiles, num_captures = search_archive(heatmap_aoi, search_start_date, search_end_date)
            
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            if num_tiles > 0:
                latest_cloud_free_tiles = create_cloud_free_basemap(tiles_gdf)
            
                m = create_folium_basemap(latest_cloud_free_tiles)
         
                now = datetime.now().strftime("%Y-%m-%dT%H%M%SZ")
                filename = f'maps/Basemap_{now}.html'
                m.save(filename)
                webbrowser.open(filename)

                logging.warning(f"Basemap Created With Latest Cloud Free Tiles.")
            else:
                logging.warning("No tiles found!")
                continue
            
            continue
        elif user_choice == '9': # Create for heat map for cloud cover for latest tiles.
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

            # aggregation_size_km = input("Set Heat Map Aggregation Size (km):")  # get this value from the user
            
            # Define the filepath for the pickle file
            pickle_filepath = 'search_results\items_cache.pkl'
            
            # # Check if the pickle file exists
            # if os.path.exists(pickle_filepath):
            #     # Load items from the pickle file
            #     with open(pickle_filepath, 'rb') as file:
            #         items, num_tiles, num_captures = pickle.load(file)
            #     logging.warning(f"Pickle File Loaded! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            # else:
            # Fetch data and save to a pickle file
            tiles_gdf, num_tiles, num_captures = search_archive(heatmap_aoi, search_start_date, search_end_date)
            logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
            
            with open(pickle_filepath, 'wb') as file:
                pickle.dump((tiles_gdf, num_tiles, num_captures), file)
            
            if num_tiles > 0:
                aggregated_tiles_gdf = setup_GDF(tiles_gdf)
            
                fig_obj = create_heatmap_for_cloud(aggregated_tiles_gdf)
                if fig_obj:

                    fig_filename = f"maps/CloudCover_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
                    fig_obj.write_html(fig_filename)
                    fig_obj.show() 
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

