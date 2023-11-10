# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

import requests
import pandas as pd
import folium
import urllib.request
import os
import config
import re
import datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
import webbrowser

KEY_ID = config.KEY_ID #please insert your API credentials here
KEY_SECRET = config.KEY_SECRET #please insert your API credentials here

TASKS_URL = 'https://api.satellogic.com/tasking/tasks/'
PRODUCTS_URL = 'https://api.satellogic.com/tasking/products/'
CLIENTS_URL = 'https://api.satellogic.com/tasking/clients/'
DOWNLOAD_URL = 'https://api.satellogic.com/telluric/scenes/'  
HEADERS = {"authorizationToken":f"Key,Secret {KEY_ID},{KEY_SECRET}"}

def query_available_tasking_products(): # Validated
    try:
        # Use api_call to get the JSON response
        response_json = api_call(PRODUCTS_URL)
        
        # If the DataFrame contains data, return it
        if response_json is not None and not response_json.empty:
            return response_json
        else:
            print("No data returned from API.")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
        
    # Products are defined by product id. Please find below a list of all available products.
    # Product 169: 'Multispectral 70cm' is Multispectral 70cm Super resolution image also centered around a pair of coordinates  (POI only, 5x10km)    

def check_account_config(): # Validated
    try:
        # Use api_call to get the JSON response
        response_json = api_call(CLIENTS_URL)

        # If the DataFrame contains data, return it
        if response_json is not None and not response_json.empty:
            return response_json
        else:
            print("No data returned from API.")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def query_tasking_products_by_status(status=""): #Validated
    # Validate the status input and set the statusparams accordingly
    valid_statuses = ["completed", "failed", "rejected", "received"]
    if status in valid_statuses:
        statusparams = {"status": status}
    else:
        if status:  # if status is not empty and not valid, print a warning
            print(f"Warning: Invalid status '{status}'. Querying all tasks instead.")
        statusparams = {"status": ""}  # query all tasks if status is not valid or empty

    try:
        # Use api_call to get the JSON response
        response_json = api_call(TASKS_URL, params=statusparams)
        
        # If the DataFrame contains data, return it
        if response_json is not None and not response_json.empty:
            return response_json
        else:
            print("No data returned from API.")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def map_desired_tasking_location(lat, lon):
    # Create map and add task location
    mp = folium.Map(location=[lat, lon], tiles="CartoDB dark_matter", zoom_start=13)
    folium.Marker([lat, lon]).add_to(mp)
    
    # Save the map to an HTML file
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    map_filename = f'images/Tasking_Map_{now}.html' 
    os.makedirs(os.path.dirname(map_filename), exist_ok=True)
    mp.save(map_filename)
    
    # Open the HTML file in the default web browser
    webbrowser.open('file://' + os.path.realpath(map_filename))

def validate_date_range(date_range_str):
    try:
        # Split the date_range_str into start_date_str and end_date_str
        start_date_str, end_date_str = date_range_str.split()

        # Parse the date strings into datetime objects
        start_date = datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

        # Check that the start date is before the end date
        if start_date >= end_date:
            print("Start date must be before end date.")
            return False
        return True
    except ValueError as ve:
        print(f"Invalid date format: {ve}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def validate_coordinates(value):
    try:
        float_value = float(value)
        return -180 <= float_value <= 180
    except ValueError:
        return False

def validate_expected_age(value):
    pattern = re.compile(r"(\d+ days, \d{2}:\d{2}:\d{2})")
    return bool(pattern.match(value))

def validate_date(value):
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return True
    except ValueError:
        return False


def get_input(prompt, validation_func=None, default_value=None):
    while True:
        user_input = input(prompt + f" (Default: {default_value}): ") or default_value
        if validation_func and not validation_func(user_input):
            print("Invalid input, please try again.")
        else:
            return user_input

def gather_task_inputs():
    # Gather inputs from the user or allow for defaults
    now = datetime.now().strftime('%Y%m%d_%H%M%S')  
    project_name = get_input("Enter the project name:", default_value=f"API_Testing")
    task_name = get_input("Enter the task name:", default_value=f"API_Task_{now}")

    product = int(get_input("Enter the product number (169):", validation_func=lambda x: x.isdigit(), default_value="169"))
    max_captures = int(get_input("Enter the maximum number of captures (1):", validation_func=lambda x: x.isdigit(), default_value="1"))
    expected_age = get_input("Enter the expected age (7 days, 00:00:00):", validation_func=validate_expected_age, default_value="7 days, 00:00:00")
    
    print("Enter the target coordinates (lat/long, dec deg):")
    lat_lon = input("Enter the latitude and longitude (format: lat,lon): ")
    lat, lon = map(float, lat_lon.split(','))  # This will split the input string into lat and lon, and convert them to floats
    
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')  # Get the current UTC date and time in the desired format
    one_month_later = (datetime.utcnow() + relativedelta(months=1)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Get the date and time one month later in the desired format

    # start_date = get_input("Enter the capture window start(YYYY-MM-DDThh:mm:ssZ):", validation_func=validate_date, default_value=now)
    # end_date = get_input("Enter the capture window end (YYYY-MM-DDThh:mm:ssZ):", validation_func=validate_date, default_value=one_month_later)

    # Set a default date range
    default_date_range = f"{now} {one_month_later}"

    date_input = get_input(
        f"Enter the capture window start and end (format: {default_date_range}):",
        validation_func=validate_date_range,  # You'll need to define this function
        default_value=default_date_range
    )

    # Split the input into start and end dates
    start_date, end_date = date_input.split()

    task = {
        "project_name": project_name,
        "task_name": task_name,
        "product": product,
        "max_captures": max_captures,
        "expected_age": expected_age,
        "target": {
            "type": "Point",
            "coordinates": [lon, lat]
        },
        "start": start_date,
        "end": end_date
    }
    print(task)
    map_desired_tasking_location(lat, lon)
    return task

def create_new_tasking(task=None): # Validated
    try:
        if not task:
            # Gather tasking inputs from the command line if the caller didn't specify the task parameters.
            task = gather_task_inputs()

        # Use api_call to get the JSON response
        response_json = api_call(TASKS_URL, method="POST", json_data=task)

        # Convert to a DataFrame and return
        return pd.json_normalize(response_json)
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def cancel_task(task_id): #Validated
    try:
        URLCancel = f'https://api.satellogic.com/tasking/tasks/{task_id}/cancel/'  # task_id of the capture

        # Use api_call to get the JSON response
        response_json = api_call(URLCancel, method="PATCH")

        # Convert to a DataFrame and return
        return pd.json_normalize(response_json)
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def check_task_status(task_id): # Validated
    try:
        # URLStatus = f'{TASKS_URL}/{task_id}/captures'  # task_id of the capture
        URLStatus = f'{TASKS_URL}{task_id}/'
        print(f"URLStatus: {URLStatus}")
        # Use api_call to get the JSON response
        response_json = api_call(URLStatus)
        # print(f"response_json: {response_json}")

        # Extract and return the 'status' field from the JSON response
        if 'status' in response_json:
            return response_json['status']
        else:
            print("Status not found in the response.")
            return None
    except Exception as e:
        print(f"An error occurred in check_task_status: {e}")
        return None

def query_and_download_image(scene_set_id, download_dir="images"):
    try:
        SSIDparams = {"sceneset_id": scene_set_id}
        method = "GET"
        print(f"SSIDparam: {SSIDparams}")
        # Use api_call to get the JSON response
        response_df = api_call(DOWNLOAD_URL, method=method, params=SSIDparams)
        print(f"Response: {response_df}")
        
        # # Write the DataFrame to a JSON file for debugging
        # response_df.to_json('maps/response.json', orient='split', indent=4)
    
        # Extract the 'attachments' list of dictionaries from the first row
        attachments = response_df.iloc[0]['attachments']

        # Find the attachment with 'name' as 'delivery_zip'
        delivery_zip_attachment = next(att for att in attachments if att['name'] == 'delivery_zip')
        
        # Get the URL and file_name from the found attachment
        url = delivery_zip_attachment['url']
        file_name = delivery_zip_attachment['file_name']
        
        # Create the full file path
        file_path = os.path.join(download_dir, file_name)
        
        # Download the zip file
        urllib.request.urlretrieve(url, file_path)

        print(f"Downloaded: {file_name}")
        print(f"URL: {url}")
        return file_name
           
    except Exception as e:
        print(f"An error occurred while querying and downloading data: {e}")
        return None

# def find_tasked_images_by_polygon(geojson_gdf): #Opted to remove this.
#     try:
#         # Check if geometry was successfully extracted
#         if geojson_gdf is None:
#             print("Invalid GeoJSON Feature. Could not extract geometry.")
#             return None

#         # Convert the shapely Polygon object to a GeoJSON-like dictionary
#         geojson_geometry = mapping(search_polygon)

#         queryparams = {"footprint": json.dumps(geojson_geometry)}

#         # Use api_call to get the JSON response
#         response_json = api_call(DOWNLOAD_URL, queryparams)

#         if response_json:
#             return pd.json_normalize(response_json, record_path=['results'])
#         else:
#             print("No data returned from API.")
#             return None
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return None
####Put commented code into spotlite_main.py If you want to make this work.
# elif sub_choice == '5': # find_tasked_images_by_polygon
#                     # Open the file dialog to select the GeoJSON file
#                     print("Provide geojson polygon file to search for tasks.")
#                     root = tk.Tk()
#                     root.withdraw()
#                     geojson_filepath = filedialog.askopenfilename(title="Select GeoJSON file",
#                                                                 filetypes=[("GeoJSON files", "*.geojson")])
#                     if geojson_filepath:
#                         logging.info(f"GeoJSON file selected: {geojson_filepath}")
#                         gdf = gpd.read_file(geojson_filepath)
#                     else:
#                         logging.warning("No geojson file!")
#                         break
#                     print(f"Results: {find_tasked_images_by_polygon(gdf)}")

def api_call(url, method="GET", params=None, json_data=None):
    try:
        if method == "GET":
            print(f"GET URL, Headers, Params, json_data: {url}, {HEADERS}, {params}, {json_data}")
            response = requests.get(url, headers=HEADERS, params=params)
        elif method == "POST":
            print(f"POST Headers, Params, json_data: {HEADERS}, {params}, {json_data}")
            response = requests.post(url, headers=HEADERS, json=json_data)
        elif method == "PATCH":
            print(f"PATCH Headers, Params, json_data: {HEADERS}, {params}, {json_data}")
            response = requests.patch(url, headers=HEADERS, json=json_data)
        else:
            print(f"Unsupported method: {method}")
            return None

        response.raise_for_status()  # Check if the request was successful
        if 'results' in response.json():
            print("DEBUG: Results Exists!")
            return pd.json_normalize(response.json(), record_path=['results'])
        else:
            return response.json()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None
    