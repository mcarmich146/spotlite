# spotlite - Satellogic Imagery Discovery Support Package

## PURPOSE:
This app simplifies the engagement with Satellogic's archive and tasking APIs.


## MAIN FUNCTION ACTIVITIES
This package supports the following activities:

### Clase Spotlite:
Purpose: Serves as a unifying class to simplify working with the features.  You can still use the helper 
classes behind Spotlite if desired.
Main Methods of Interest:
1) Search And Animate Site - Creates animated stack(s) for POIs with width polygons.
2) Create Cloud Free Basemap - Creates a cloud free map using the latest cloud free tile from the archive.
3) Create Heatmap Of Collection Age - Creates heatmap for image age for AOI and date range.
4) Create Heatmap Of Imagery Depth - Creates heatmap for image depth/count for AOI and date range.
5) Create Heatmap Of Cloud Cover - Creates heatmap for image cloud cover for AOI and date range.
6) Download Tiles For BBox - Downloads the tiles for an AOI and date range
7) Run Subscription Monitor - Monitors a configurable series of AOIs for new captures and send email notifications.
8) Dump Footprints - Finds and saves the image strip footprints to the desktop for an AOI and date range.

### Class Searcher:
1) search_archive - search the archive using multi-threaded approach

### Class TileManager:
1) animate_tile_stack - animate tile stack found by Searcher class, saves results to maps/ and images/
2) cloud_heatmap - create heatmap for tiles with cloud cover below a configurable threshold
3) age_heatmap - create heatmap for tile age
4) count_heatmap - create heatmap for tile count, essentially the depth of stacks.
5) save_tiles - download and save tiles on local computer
6) filter_tiles - filter tiles based on cloud cover and valid pixel percent
7) filter_and_sort_tiles - filter_tiles plus sort and eliminate duplicates for heatmap optimization
8) create_aois_from_points - takes point list and returns bbox aois and points list

### Class Monitor Agent:
Purpose: To manage the monitoring of the configurable list of subscription areas.
1) Run - once the Agent is initialized you just need to call run and it will continue to run in the terminal until canceled.

### Class TaskingManager:
1) create_new_task - creates new task request to capture new imagery
2) cancel_task - cancels a task
3) task_status - checks the status of a task
4) download_image - download a tasked and received image
5) query_tasks_by_status - find all tasks of certain status code
6) capture_list - list the sceneset_ids for a capture task
7) check_account_config - checks the status of the user's account
8) query_available_tasking_products - check what products the user can order

