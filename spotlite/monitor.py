# Copyright (c) 2024 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of the Spotlite package.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.
# 


from typing import Tuple, Dict, Optional, List, Type
import time
from time import sleep
import schedule
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
import json
import os
import geojson
from datetime import datetime, timedelta
import base64
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from requests import HTTPError
from selenium import webdriver
from email.mime.multipart import MIMEMultipart
from shapely.geometry import Polygon, Point, box, shape
from .tile import TileManager


logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

tiles_gdf = None
    
class MonitorAgent:
    def __init__(self, key_id="", key_secret="", period=None, subscriptions_file_path=None):
        # Assigning default values to instance attributes
        self.key_id = key_id
        self.key_secret = key_secret
        if subscriptions_file_path is None:
            self.subscriptions_file_path = "databases/subscriptions.geojson"
        else:
            self.subscriptions_file_path = subscriptions_file_path

        # If Period is set then set this class's variable.
        if period is not None:
            self.period = int(period)
            self.is_period_set = True
        else:
            # Set period to 24 hours and use the new auto-run mechanism leveraging the geojson.
            # This is used to define the time span of the tile search.
            self.period = 1440  
            self.is_period_set = False
        
        self._param = None  # Initialize _param for the property
        self.tile_manager = TileManager(key_id, key_secret)

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    # Start Monitoring
    def run(self):
        """Starts the monitoring process and runs it until cancelled.
        Uses the contents of the subscriptions_file_path to drive the behavior."""
        
        logger.warning(f"Using Subscription File: {self.subscriptions_file_path}")

        # Load subscription data
        data = self.load_subscriptions()

        # Schedule tasks based on times in JSON file
        for feature in data['features']:
            if self.is_period_set is False: #doing time of day processing.
                time_of_day = feature['properties'].get('local_processing_time')
                if time_of_day:
                    # self.check_and_notify(feature)
                    schedule.every().day.at(time_of_day).do(self.check_and_notify, feature)
                else:
                    logger.error("Period mechanism using time of day to schedule, but time is not set correctly in subscriptions file.")
            else: # doing periodic searches.
                # Set up the period between monitoring runs.
                schedule.every(self.period).minutes.do(self.check_and_notify, feature)

        # Keep running the schedule in a loop
        while True:
            schedule.run_pending()
            time.sleep(1)

    def check_and_notify(self, feature):        
        timer_start = datetime.now()
        user_emails = feature['properties']['emails']  # Now we have a list
        subscription_name = feature['properties']['subscription_name']
        
        geojson_polygon = feature['geometry']
        shapely_polygon = shape(geojson_polygon)  # Convert GeoJSON to Shapely Polygon
        minx, miny, maxx, maxy = shapely_polygon.bounds  # Get the bounding box coordinates
        aoi = box(minx, miny, maxx, maxy) 

        foundTiles, _, sorted_aggregated_df, footprints_geojson_path, previews_filenames = self._check_archive(aoi, self.period, subscription_name)  # adjusted to receive gdf_grouped
        if foundTiles == True:
            email_body, email_subject= self._format_email_body_subject(subscription_name, sorted_aggregated_df)
            to_emails = ', '.join(user_emails)  # Join all emails into a single string
            self._send_email(to_emails, email_subject, email_body, footprints_geojson_path, previews_filenames)
        else:
            logger.error(f"No Images Found In This Search Polygon.")
        
        timer_end = datetime.now()
        duration = timer_end - timer_start
        logger.warning(f"Search Completed At: {timer_end} local time.")
        logger.warning(f"Search Processing Duration: {duration}")
        logger.warning(f"\nPausing Until Next Monitoring Run in {self.period} minutes.")

    def _check_archive(self, aoi_box: Polygon, period: int, subsription_name: str):   
        # Create the search window in UTC 
        end_date = datetime.utcnow()  # Current date and time in UTC
        start_date = end_date - timedelta(minutes=period)  
        # start_date = end_date - timedelta(weeks=1)

        # Formatting dates to string as your `search_archive` might expect string input
        str_start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
        str_end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')
        logger.warning(f"\nSearching: {subsription_name} \nPeriod: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} and {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')} \nAOI: {aoi_box}")
        tiles_gdf, num_tiles, num_captures = self.tile_manager.get_tiles(aoi_box, str_start_date, str_end_date)

        logging.warning(f"Search complete! Num Tiles: {num_tiles}, Num Captures: {num_captures}")
        
        # If no tiles found then return False.
        if num_tiles == 0:
            return False, 0, None, None, None  
        
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

        # write out the footprints file
        now = datetime.now()
        str_start_date = start_date.strftime('%Y-%m-%dT%H-%M-%S')
        str_end_date = end_date.strftime('%Y-%m-%dT%H-%M-%S')
        footprints_filename = f"maps/Footprints_{str_start_date}-{str_end_date}_Created-{now.strftime('%Y-%m-%d_%H-%M-%S')}.geojson"
        
        try:
            output_gdf.to_file(footprints_filename, driver='GeoJSON')
            logger.debug(f"Footprint File Saved: {footprints_filename}")
        except Exception as e:  # Correct syntax and catch general exception
            logging.error(f"Failed to write footprint file: {e}")
            return False, 0, None, None, None

        # Sort the DataFrame by capture_date in descending order
        sorted_aggregated_df = output_gdf.sort_values(by='capture_date', ascending=False)

        previews_filenames = self.tile_manager.create_preview_jpegs(tiles_gdf)

        return True, len(tiles_gdf), sorted_aggregated_df, footprints_filename, previews_filenames
        
    def _send_email(self, to_email, subject, body, footprints_geojson_path:str, previews_filenames: List):
        service = self._build_service()
        
        # Create a MIMEMultipart message
        msg = MIMEMultipart()
        msg['To'] = ""
        msg['Bcc'] = to_email
        msg['Subject'] = subject

        # Attach the body text
        msg.attach(MIMEText(body, 'html'))

        now = datetime.utcnow()
        # Attach the footprints file
        if footprints_geojson_path:
            with open(footprints_geojson_path, 'r') as f:
                mime_json = MIMEText(f.read(), 'application/json')
                mime_json.add_header("Content-Disposition", "attachment", filename=f"Footprints_{now.strftime('%Y-%m-%dT%H-%M-%SZ')}.geojson")
                msg.attach(mime_json)

            # Attach each preview JPEG
        for filename in previews_filenames:
            with open(filename, 'rb') as f:
                mime_image = MIMEImage(f.read())
                mime_image.add_header("Content-Disposition", "attachment", filename=os.path.basename(filename))
                msg.attach(mime_image)

        raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        create_message = {'raw': raw_msg}

        try:
            message = (service.users().messages().send(userId="me", body=create_message).execute())
            logger.warning(f'Sent message to {to_email}, Message Id: {message["id"]}')
        except HTTPError as error:
            logger.error(f'An error occurred: {error}')
            message = None

    def _format_email_body_subject(self, subscription_name, sorted_aggregated_df):
        email_body = f"""
        <html>
            <body>
                <p>Dear Subscriber,</p>
                <p>We are excited to inform you that new imagery has been found in your subscription area: {subscription_name}.</p>
                <p>Here are the details of the new images - {len(sorted_aggregated_df)} New Captures:</p>
                <ul>
        """
        # Iterate over rows in the sorted_aggregated_df DataFrame
        for index, row in sorted_aggregated_df.iterrows():
            # Extract data from each row
            outcome_id = row['outcome_id']
            actual_capture_date = row['capture_date']
            cloud_cover_percentage = int(row['cloud_cover_mean'])  # Assuming mean_cloud_cover is in decimal form

            # Format the date if it's a datetime object
            if isinstance(actual_capture_date, datetime):
                actual_capture_date = actual_capture_date.strftime('%Y-%m-%d %H:%M:%S UTC')

            # Append information to the email body
            email_body += f"<li>Capture Date: {actual_capture_date}, Outcome ID: {outcome_id}, Cloud Cover: {cloud_cover_percentage}%</li>"

        email_body += """
                </ul>
                <p>Thank you for choosing our service!</p>
                <p>Best Regards From Your Team At Satellogic.</p>
                <p></p>
                <p>Note: The attached geojson file contains the footprints of the listed captures.</p>
            </body>
        </html>
        """

        email_subject = f"DoNotReply - New Satellogic Imagery - {subscription_name}"
        return email_body, email_subject


    def _save_screen_shot(self, input_html_file_path, output_png_file_path, is_cloud):
        options = webdriver.ChromeOptions()
        # Disabling 3D APIs as per the solution found for WebGL error
        # options.add_argument('--disable-3d-apis')
        options.add_argument('headless')
        with webdriver.Chrome(options=options) as driver:
            driver.get(f'file:///{os.path.abspath(input_html_file_path)}')  # Provide absolute path with file:///
            sleep(5)
            
            if is_cloud == True:
                # Set zoom
                driver.execute_script("document.body.style.zoom='70%'")
                # Get the dimensions of the body and the window
                body_width = driver.execute_script("return document.body.scrollWidth")
                body_height = driver.execute_script("return document.body.scrollHeight")
                window_width = driver.execute_script("return window.innerWidth")
                window_height = driver.execute_script("return window.innerHeight")

                # Calculate center position
                center_x = (body_width - window_width) // 2
                center_y = (body_height - window_height) // 2

                # Scroll to center
                driver.execute_script(f"window.scrollTo({center_x}, {center_y})")
            driver.save_screenshot(output_png_file_path)

    def _compute_centroid(self, aoi_polygon):
        polygon_shape = shape(aoi_polygon)
        centroid = polygon_shape.centroid
        return centroid.y, centroid.x     

    def _build_service(self):
        # flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
        # creds = flow.run_local_server(port=0)

        # service = build('gmail', 'v1', credentials=creds)
        creds = None
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        return build('gmail', 'v1', credentials=creds)

    def load_subscriptions(self, input_subc_file_path=None):
        if input_subc_file_path != None:
            subc_path = input_subc_file_path
        else:
            subc_path = self.subscriptions_file_path

        # Check if the file exists
        if not os.path.exists(subc_path):
            logger.error(f"Subscription DB not present: {subc_path}")
            return None  # Or handle the error as needed

        with open(subc_path, 'r') as f:
            data = json.load(f)
        return data

    def save_subscriptions(self, data, target_path=None):
        if target_path != None:
            subc_path = target_path
        else:
            subc_path = self.subscriptions_file_path
        
        with open(subc_path, 'w') as f:
            json.dump(data, f, indent=2)

    def list_subscriptions(self):
        data = self.load_subscriptions()
        for sub in data['features']:
            subscription_id = sub['id']
            subscription_name = sub['properties']['subscription_name']
            emails = sub['properties']['emails']  # Accessing the list of emails
            polygon = sub['geometry']
            emails_str = ', '.join(emails) 
            print(f"ID: {subscription_id}, Name: {subscription_name}, \nEmails: {emails_str}, \nPolygon: {polygon}")

    def add_subscription(self, user_emails: List[str], subscription_name: str, polygon: Polygon):  
        data = self.load_subscriptions()
        feature_collection = geojson.FeatureCollection(data['features'])

        new_feature = geojson.Feature(
            geometry=polygon,
            properties={
                'emails': user_emails,  # Storing multiple emails
                'subscription_name': subscription_name
            },
            id=str(len(feature_collection['features']) + 1)
        )

        feature_collection['features'].append(new_feature)
        self.save_subscriptions(feature_collection)

    def delete_subscription(self, subscription_id):
        data = self.load_subscriptions()
        updated_features = [feature for feature in data['features'] if feature['id'] != subscription_id]
        data['features'] = updated_features
        self.save_subscriptions(data)

    def delete_all_subscriptions(self, user_email):
        data = self.load_subscriptions()
        data[user_email] = []
        self.save_subscriptions(data)  # Adjusted argument

    def add_subscription_from_file(self, user_email, name, geojson_file_path):
        with open(geojson_file_path, 'r') as f:
            geojson_data = json.load(f)
        polygon = geojson_data['features'][0]['geometry']['coordinates']
        self.add_subscription(user_email, name, polygon)