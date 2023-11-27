# spotlite - Satellogic Imagery Discovery Support Package

## PURPOSE:
This app simplifies the engagement with Satellogic's archive and tasking APIs.


## MAIN FUNCTION ACTIVITIES
This package supports the following activities:
  Class Searcher:
    search_archive - search the archive using multi-threaded approach
    save_tiles - download and save tiles on local computer
    filter_tiles - filter tiles based on cloud cover and valid pixel percent
    filter_and_sort_tiles - filter_tiles plus sort and eliminate duplicates for heatmap optimization
    create_aois_from_points - takes point list and returns bbox aois and points list
  Class Visualizer:
    animate_tile_stack - animate tile stack found by Searcher class, saves results to maps/ and images/
    cloud_heatmap - create heatmap for tiles with cloud cover below a configurable threshold
    age_heatmap - create heatmap for tile age
    count_heatmap - create heatmap for tile count, essentially the depth of stacks.
  Class TaskingManager:
    create_new_task - creates new task request to capture new imagery
    cancel_task - cancels a task
    task_status - checks the status of a task
    download_image - download a tasked and received image
    query_tasks_by_status - find all tasks of certain status code
    check_account_config - checks the status of the user's account
    query_available_tasking_products - check what products the user can order

