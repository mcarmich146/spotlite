# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

from time import sleep
import json
import geopandas as gpd
from shapely.geometry import shape
import os
import uuid
import smtplib
import json
import geojson
from datetime import datetime, timedelta
from satellogicUtils import search_archive, group_by_capture
from mapUtils import create_map, update_map_with_tiles, create_heatmap_for_cloud
import config
import base64
from email.mime.text import MIMEText
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from requests import HTTPError
from selenium import webdriver
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
subcDb = config.SUBSCRIPTIONS_FILE_PATH
password = config.EMAIL_PASSWORD
from_email = config.EMAIL_ADDRESS
period = config.SUBC_MON_FREQUENCY # In Minutes

def build_service():
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

def load_subscriptions(input_subc_file_path=None):
    if input_subc_file_path != None:
        subc_path = input_subc_file_path
    else:
        subc_path = subcDb

    with open(subc_path, 'r') as f:
        data = json.load(f)
    return data

def save_subscriptions(data):
    with open(subcDb, 'w') as f:
        json.dump(data, f, indent=2)

def list_subscriptions():
    data = load_subscriptions()
    for sub in data['features']:
        subscription_id = sub['id']
        subscription_name = sub['properties']['subscription_name']
        polygon = sub['geometry']
        print(f"ID: {subscription_id}, Name: {subscription_name}, Polygon: {polygon}")

def add_subscription(user_emails, subscription_name, polygon):  # user_emails is now a list
    data = load_subscriptions()
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
    save_subscriptions(feature_collection)

def delete_subscription(sub_id):
    data = load_subscriptions()
    updated_features = [feature for feature in data['features'] if feature['id'] != sub_id]
    data['features'] = updated_features
    save_subscriptions(data)

def delete_all_subscriptions(user_email):
    data = load_subscriptions()
    data[user_email] = []
    save_subscriptions(data)  # Adjusted argument

def add_subscription_from_file(user_email, name, geojson_file_path):
    with open(geojson_file_path, 'r') as f:
        geojson_data = json.load(f)
    polygon = geojson_data['features'][0]['geometry']['coordinates']
    add_subscription(user_email, name, polygon)

def send_email(to_email, subject, body, folium_png_path=None, folium_html_path=None, plotly_html_path=None, plotly_png_path=None):
    service = build_service()
    
    # Create a MIMEMultipart message
    msg = MIMEMultipart()
    msg['to'] = to_email
    msg['subject'] = subject

    # Attach the body text
    msg.attach(MIMEText(body, 'html'))

    # Attach the tiles image
    if folium_png_path:
        with open(folium_png_path, 'rb') as img:
            mime_img = MIMEImage(img.read())
            mime_img.add_header('Content-ID', 'TilesMap')  # The image ID should match the one used in the body
            mime_img.add_header("Content-Disposition", "attachment", filename="tiles_map.png")
            msg.attach(mime_img)
        
    # Attach the cloud image
    if plotly_png_path:
        with open(plotly_png_path, 'rb') as img:
            mime_img = MIMEImage(img.read())
            mime_img.add_header('Content-ID', 'CloudMap')  # The image ID should match the one used in the body
            mime_img.add_header("Content-Disposition", "attachment", filename="cloud_map.png")
            msg.attach(mime_img)
    
    # Attach the HTML map
    if folium_html_path:
        with open(folium_html_path, 'r') as f:
            mime_html = MIMEText(f.read(), 'html')
            mime_html.add_header("Content-Disposition", "attachment", filename="tiles_map.html")
            msg.attach(mime_html)

    # Attach the Cloud HTML map   
    if plotly_html_path:
        with open(plotly_html_path, 'r', encoding='utf-8') as f:
            mime_html = MIMEText(f.read(), 'html')
            mime_html.add_header("Content-Disposition", "attachment", filename="cloud_cover_heatmap.html")
            msg.attach(mime_html)

    raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    create_message = {'raw': raw_msg}

    try:
        message = (service.users().messages().send(userId="me", body=create_message).execute())
        print(f'Sent message to {to_email}, Message Id: {message["id"]}')
    except HTTPError as error:
        print(f'An error occurred: {error}')
        message = None

def format_email_body(subscription_name, gdf_grouped):
    email_body = f"""
    <html>
        <body>
            <p>Dear Subscriber,</p>
            <p>We are excited to inform you that new imagery has been found in your subscription area: {subscription_name}.</p>
            <p>Here are the details of the new images:</p>
            <ul>
    """
    for (capture_date, outcome_id), group in gdf_grouped:
        actual_capture_date = group.iloc[0]['capture_date']
        # cloud_cover = group.iloc[0].get('properties', {}).get('eo:cloud_cover', 'N/A')
        # email_body += f"<li>Capture Date: {date.strftime('%Y-%m-%d')}, <a href='https://your-image-url-base/{outcome_id}'>Outcome ID: {outcome_id}</a>, Cloud Cover: {cloud_cover}</li>"
        email_body += f"<li>Capture Date: {actual_capture_date.strftime('%Y-%m-%d %H:%M:%S UTC')}, Outcome ID: {outcome_id}</li>"

    email_body += f"""
            </ul>
            <p>Thank you for choosing our service!</p>
            <p>Best Regards From Your Team At Satellogic.</p>
            <p></p>
            <p>Note: to open html map with cloud information, it must be downloaded first.</p>
        </body>
    </html>
    """
    return email_body

def check_and_notify():
    data = load_subscriptions()
    for feature in data['features']:
        user_emails = feature['properties']['emails']  # Now we have a list
        subscription_name = feature['properties']['subscription_name']
        aoi_polygon = feature['geometry']

        foundTiles, _, gdf_grouped, map_image_path_png, map_image_path_html, plotly_map_path_html, plotly_map_path_png = check_archive(aoi_polygon, period, subscription_name)  # adjusted to receive gdf_grouped
        if foundTiles == True:
            email_body = format_email_body(subscription_name, gdf_grouped)
            to_emails = ', '.join(user_emails)  # Join all emails into a single string
            send_email(to_emails, 'New Satellogic Imagery Available', email_body, map_image_path_png, map_image_path_html, plotly_map_path_html, plotly_map_path_png)
        else:
            print(f"No Images Found In The Last {period} Minutes.")

def check_archive(aoi_polygon, period, subsription_name):   
    # Create the search window in UTC 
    end_date = datetime.utcnow()  # Current date and time in UTC
    start_date = end_date - timedelta(minutes=period)  
    # start_date = end_date - timedelta(weeks=4)  

    # Formatting dates to string as your `search_archive` might expect string input
    str_start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    str_end_date = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    print(f"Searching {subsription_name} Between {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} and {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')} Every {period} minutes in UTC.")
    
    tiles_gdf, item_count, num_captures = search_archive(aoi_polygon, str_start_date, str_end_date)
    if tiles_gdf:
        # print("Found matching tiles...")
        # Transform the result to a geodataframe to easily manipulate and explore the data
        grouped = group_by_capture(tiles_gdf)

        # Create the map
        lat, lon = compute_centroid(aoi_polygon) 
        folium_map = create_map(lat, lon, aoi_polygon)
        folium_map = update_map_with_tiles(folium_map, tiles_gdf)  # Adds all the tiles on the map
        cloud_map = create_heatmap_for_cloud(tiles_gdf)
       
        # Save the map as an image
        map_image_path_html = f'maps/folium-map-for-email.html'
        map_image_path_png = f'maps/folium-map-for-email.png'
        folium_map.save(map_image_path_html)
        save_screen_shot(map_image_path_html, map_image_path_png, False)

        cloud_map_path_html = f'maps/cloud-map-for-email.html'
        cloud_map_path_png = f'maps/cloud-map-for-email.png'
        cloud_map.write_html(cloud_map_path_html)
        # cloud_map.write_image(cloud_map_path_png)
        save_screen_shot(cloud_map_path_html, cloud_map_path_png, True)
                    
        return True, len(tiles_gdf), grouped, map_image_path_png, map_image_path_html, cloud_map_path_html, cloud_map_path_png  # adjusted to return True instead of items
    return False, 0, None, None, None, None, None  # adjusted to return 0 and None for consistency

def save_screen_shot(input_html_file_path, output_png_file_path, is_cloud):
    options = webdriver.ChromeOptions()
    # Disabling 3D APIs as per the solution found for WebGL error
    # options.add_argument('--disable-3d-apis')
    options.add_argument('headless')
    with webdriver.Chrome(options=options) as driver:
        driver.get(f'file:///{os.path.abspath(input_html_file_path)}')  # Provide absolute path with file:///
        sleep(2)
        
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

def compute_centroid(aoi_polygon):
    polygon_shape = shape(aoi_polygon)
    centroid = polygon_shape.centroid
    return centroid.y, centroid.x 
