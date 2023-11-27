# spotlite - Satellogic Imagery Discovery Support Package

## PURPOSE:
This app simplifies the engagement with Satellogic's archive and tasking APIs.


## MAIN FUNCTION ACTIVITIES
This package supports the following activities:

### Class Searcher:
1) search_archive - search the archive using multi-threaded approach
2) save_tiles - download and save tiles on local computer
3) filter_tiles - filter tiles based on cloud cover and valid pixel percent
4) filter_and_sort_tiles - filter_tiles plus sort and eliminate duplicates for heatmap optimization
5) create_aois_from_points - takes point list and returns bbox aois and points list

### Class Visualizer:
1) animate_tile_stack - animate tile stack found by Searcher class, saves results to maps/ and images/
2) cloud_heatmap - create heatmap for tiles with cloud cover below a configurable threshold
3) age_heatmap - create heatmap for tile age
4) count_heatmap - create heatmap for tile count, essentially the depth of stacks.

### Class TaskingManager:
1) create_new_task - creates new task request to capture new imagery
2) cancel_task - cancels a task
3) task_status - checks the status of a task
4) download_image - download a tasked and received image
5) query_tasks_by_status - find all tasks of certain status code
6) capture_list - list the sceneset_ids for a capture task
7) check_account_config - checks the status of the user's account
8) query_available_tasking_products - check what products the user can order

