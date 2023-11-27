# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

import json
import requests
import pandas as pd
import folium
import urllib.request
import os
import re
import datetime
from datetime import datetime
import webbrowser
import logging

logger = logging.getLogger(__name__)

class TaskingManager:
    def __init__(self, key_id="", key_secret=""): #must initialize with Keys.
        self.tasks_url = "https://api.satellogic.com/tasking/tasks/"
        self.products_url = "https://api.satellogic.com/tasking/products/"
        self.clients_url = "https://api.satellogic.com/tasking/clients/"
        self.download_url = "https://api.satellogic.com/telluric/scenes/"
        self.key_id = key_id
        self.key_secret = key_secret
        self.headers = {"authorizationToken":f"Key,Secret {self.key_id},{self.key_secret}"}
        self._param = None  # Initialize _param for the property


    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value


    def query_available_tasking_products(self): # Validated
        try:
            # Use api_call to get the JSON response
            response_json = self._api_call(self.products_url)

            # If the DataFrame contains data, return it
            if response_json is not None and not response_json.empty:
                return response_json
            else:
                logger.info("No data returned from API.")
                return None
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return None

        # Products are defined by product id. Please find below a list of all available products.
        # Product 169: 'Multispectral 70cm' is Multispectral 70cm Super resolution image also centered around a pair of coordinates  (POI only, 5x10km)

    def check_account_config(self): # Validated
        try:
            # Use api_call to get the JSON response
            response_json = self._api_call(self.clients_url)

            # If the DataFrame contains data, return it
            if response_json is not None and not response_json.empty:
                return response_json
            else:
                logger.info("No data returned from API.")
                return None
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return None

    def query_tasks_by_status(self, status=""): #Validated
        """Validate the status input and set the statusparams accordingly."""

        valid_statuses = ["completed", "failed", "rejected", "received", "canceled"]
        if status in valid_statuses:
            statusparams = {"status": status}
        else:
            if status:  # if status is not empty and not valid, print a warning
                logger.info(f"Warning: Invalid status '{status}'. Querying all tasks instead.")
            statusparams = {"status": ""}  # query all tasks if status is not valid or empty

        try:
            # Use api_call to get the JSON response
            response_json = self._api_call(self.tasks_url, params=statusparams)

            # If the DataFrame contains data, return it
            if response_json is not None and not response_json.empty:
                return response_json
            else:
                logger.info("No data returned from API.")
                return None
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return None

    def map_capture_location(self, lat, lon):
        """Create a map of the POI task created.  Returns a folium map object."""
        # Create map and add task location
        mp = folium.Map(location=[lat, lon], tiles="CartoDB dark_matter", zoom_start=13)
        folium.Marker([lat, lon]).add_to(mp)

        # Save the map to an HTML file
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        map_filename = f'images/Tasking_Map_{now}.html'
        os.makedirs(os.path.dirname(map_filename), exist_ok=True)
        mp.save(map_filename)

        # Open the HTML file in the default web browser
        # webbrowser.open('file://' + os.path.realpath(map_filename))
        return mp

    def _validate_date_range(self, date_range_str):
        try:
            # Split the date_range_str into start_date_str and end_date_str
            start_date_str, end_date_str = date_range_str.split()

            # Parse the date strings into datetime objects
            start_date = datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M:%SZ')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%dT%H:%M:%SZ')

            # Check that the start date is before the end date
            if start_date >= end_date:
                logger.info("Start date must be before end date.")
                return False
            return True
        except ValueError as ve:
            logger.info(f"Invalid date format: {ve}")
            return False
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return False

    def _validate_coordinates(self, value):
        try:
            float_value = float(value)
            return -180 <= float_value <= 180
        except ValueError:
            return False

    def _validate_expected_age(self, value):
        pattern = re.compile(r"(\d+ days, \d{2}:\d{2}:\d{2})")
        return bool(pattern.match(value))

    def _validate_date(self, value):
        try:
            datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
            return True
        except ValueError:
            return False

    def create_new_tasking(self, task): # Validated
        """User needs to pass the following structure in to place the tasting order:
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
            """
        try:
            # Use api_call to get the JSON response
            response_json = self._api_call(self.tasks_url, method="POST", json_data=task)

            # Convert to a DataFrame and return
            return pd.json_normalize(response_json)
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return None


    def cancel_task(self, task_id): #Validated
        try:
            URLCancel = f'https://api.satellogic.com/tasking/tasks/{task_id}/cancel/'  # task_id of the capture

            # Use api_call to get the JSON response
            response_json = self._api_call(URLCancel, method="PATCH")

            # Convert to a DataFrame and return
            return pd.json_normalize(response_json)
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            return None


    def task_status(self, task_id): # Validated
        try:
            # URLStatus = f'{TASKS_URL}/{task_id}/captures'  # task_id of the capture
            URLStatus = f'{self.tasks_url}{task_id}/'
            
            # Use api_call to get the JSON response
            response_json = self._api_call(URLStatus)
            
            # Extract and return the 'status' field from the JSON response
            if 'status' in response_json:
                return response_json['status']
            else:
                logger.info("Status not found in the response.")
                return None
        except Exception as e:
            logger.warning(f"An error occurred in task_status: {e}")
            return None

    def capture_list(self, task_id):
        try:
            # URLStatus = f'{TASKS_URL}/{task_id}/captures'  # task_id of the capture
            URLStatus = f'{self.tasks_url}{task_id}/captures/'
            
            # Use api_call to get the JSON response
            response_json = self._api_call(URLStatus)
            print(URLStatus)

            # Return the response which is a table of captures
            return response_json

        except Exception as e:
            logger.warning(f"An error occurred in capture_list: {e}")
            return None

    def _ensure_dir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)


    def download_image(self, scene_set_id, download_dir="images"):
        try:
            if download_dir is None:
                download_dir = "images/"

            self._ensure_dir(download_dir)

            SSIDparams = {"sceneset_id": scene_set_id}
            method = "GET"
            logger.debug(f"SSIDparam: {SSIDparams}")
            # Use api_call to get the JSON response
            url = self.download_url 
            response_df = self._api_call(url, method=method, params=SSIDparams)
            
            if response_df.empty:
                logger.info("No data found in response.")
                return None

            logger.debug(f"Response: {response_df}")

            # Extract the 'attachments' list of dictionaries from the first row
            attachments = response_df.iloc[0]['attachments']

            # Find the attachment with 'name' as 'delivery_zip'
            delivery_zip_attachment = next(att for att in attachments if att['name'] == 'delivery_zip')

            logger.debug(f"delivery_zip_zttachment: {delivery_zip_attachment}")

            # Get the URL and file_name from the found attachment
            url = delivery_zip_attachment['url']
            file_name = delivery_zip_attachment['file_name']
            # logger.info(f"file_name: {file_name}")

            # logger.info(f"download_dir: {download_dir}")
            # Create the full file path to save to.
            file_path = os.path.join(download_dir, file_name)

            # logger.info(f"file_path: {file_path}")

            # Download the zip file
            saved_file, httpmessage = urllib.request.urlretrieve(url, file_path)

            logger.info(f"Final Filename: {saved_file}")
            logger.info(f"httpmessage: {httpmessage}")

            logger.info(f"Downloaded: {file_name}")
            logger.info(f"URL: {url}")
            return file_name

        except Exception as e:
            logger.info(f"An error occurred while querying and downloading data: {e}")
            return None

    def _api_call(self, url, method="GET", params=None, json_data=None):
        HEADERS = self.headers
        try:
            if method == "GET":
                logger.info(f"GET URL, Headers, Params, json_data: {url}, {HEADERS}, {params}, {json_data}")
                response = requests.get(url, headers=HEADERS, params=params)
            elif method == "POST":
                logger.info(f"POST Headers, Params, json_data: {HEADERS}, {params}, {json_data}")
                response = requests.post(url, headers=HEADERS, json=json_data)
            elif method == "PATCH":
                logger.info(f"PATCH Headers, Params, json_data: {HEADERS}, {params}, {json_data}")
                response = requests.patch(url, headers=HEADERS, json=json_data)
            else:
                logger.info(f"Unsupported method: {method}")
                return None

            response.raise_for_status()  # Check if the request was successful
            if 'results' in response.json():
                logger.debug(response.json())
                return pd.json_normalize(response.json(), record_path=['results'])
            else:
                return response.json()
        except requests.RequestException as e:
            logger.info(f"API request failed: {e}")
            return None
