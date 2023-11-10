import json
import schedule
import time
from subscriptionUtils import send_email
from mapUtils import process_multiple_points_to_bboxs, update_map_with_tiles
from satellogicUtils import search_archive, save_and_animate_tiles, create_single_image_html, group_items_into_GPDF, group_by_capture
import config
from datetime import datetime, timedelta
import geopandas as gpd
import urllib.parse
import logging

logger = logging.getLogger(__name__)

FROM_EMAIL = config.EMAIL_ADDRESS
PERIOD = config.SUBC_MON_FREQUENCY # In Minutes
POINTS_FILE_PATH = config.MONITORING_POINTS_FILE_PATH

#&& Indications and Warnings Demo App (IWDemo) &&
# Setup:
# -This app assumes you enter a geojson point file created for all the airbases
# -You add the file to a file in the app directory call. pointTargetsToMonitor.  
# -Files should be on separate lines with no formating including
#   relative path to files. eg points_of_interest\<filename>
# Systematic Search:
# -It then creates the polygon bounding box around each point with a consistent size
# -Then searches archive for new images that have come in during the last period
#   over each point (period defined in config, eg 6 hours).  
# -If it finds an image or multiple images it searches the archive again for the past month
# to find the most recent image outside the Period to build a before and after. 
# -Then mosaics and saves the images, discarding cloudy images, since it needs a before
# -It will seek for the most recent cloud free image tiles.
# Image Preparation: 
# -Creates a before and after image pair and saves as a single side-by-side image.
# -Creates a report for the user using GPT to asses the imagery and comments.
# -Creates and sends an email with the consolidated list of images found, their before and afters
#   and the point map being searched as an image attachment.  
# -Each before an after should have a filename of its <CaptureDate>_<lat>_<long>_BnA.JPEG

def format_email_body(capture_points_filename, found_images_data):
    email_body = """
    <html>
        <body>
            <p>Dear Subscriber,</p>
            <p>We are excited to inform you that new imagery has been found for your subscription points file: {}.</p>
            <p>Here are the details of the new images:</p>
            <ul>
    """.format(capture_points_filename)

    # Use the new found_images_data structure here
    for data in found_images_data:
        # Format cloud cover to 2 decimal places and append '%'
        data['Cloud_Cover'] = "{:.0f}%".format(data['Cloud_Cover'])
        animation_uri = urllib.parse.quote(data['Animation_Abs_Path'])
        file_uri = 'file:///' + data['Animation_Abs_Path'].replace('\\', '/')
        email_body += '<li>Capture Date: {}, Country: {}, Outcome ID: <a href="{}">{}</a>, AOI Lat_Long: {}, {}, Cloud Cover: {}</li>'.format(
            data['Capture Date'].strftime('%Y-%m-%d %H:%M:%S UTC'),
            data['Country'],
            file_uri,
            data['Outcome ID'],
            data['AOI Lat_Long'][0],
            data['AOI Lat_Long'][1],
            data['Cloud_Cover']
        )

    email_body += """
            </ul>
            <p>Thank you for choosing our service! - 11:21</p>
            ...
        </body>
    </html>
    """
    return email_body 

def check_and_notify_points():
    global POINTS_FILE_PATH
    # Read the GeoJSON file
    with open(POINTS_FILE_PATH, 'r') as f:
        geojson_data = json.load(f)
    
    # Collect features into an array.  These are the user's collections of POIs.
    features_array = geojson_data['features']

    # Call check_and_notify_points once for each feature in the array then update the json feature file.
    for features_index, points_feature in enumerate(features_array):
        # Get the boxes to search
        bbox_width = config.DEFAULT_BOUNDING_BOX_WIDTH
        last_search_datetime = points_feature['properties'].get('timeOfLastSearchUtc', None) 
        country = points_feature['properties'].get('country', None) 
        logger.warning(f"Checking Region: {country}.") 
        logger.info(f"Time of last search: {last_search_datetime}")
        # Check if the field exists before converting it to datetime
        if last_search_datetime:
            last_search_datetime = datetime.strptime(last_search_datetime, '%Y-%m-%dT%H:%M:%SZ')
        else:
            raise ValueError("LastSearchDateTime is Invalid.")   # or any other default value

        points_filename = points_feature['properties'].get('filename', None)
        
        logger.info(f"Processing Points File: {points_filename}")
        
        gdf = gpd.read_file(points_filename)
        points = [{'lat': row.geometry.y, 'lon': row.geometry.x} for index, row in gdf.iterrows()]

        if not points:
            raise ValueError("Points are undefined or empty!!!") # This should never happen. 

        # Create map of points and bounding boxes
        master_map, aois_list = process_multiple_points_to_bboxs(points, float(bbox_width))

        logger.info(f"Number Of AOIs To Be Searched: {len(aois_list)}")

        # Update time and search images
        end_date = datetime.utcnow()
        start_date = min(last_search_datetime, end_date - timedelta(minutes=config.SUBC_MON_FREQUENCY))
        # last_search_datetime = end_date - timedelta(days=30)
        start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info(f"Passing Into Search: {start_date_str}, {end_date_str}")

        found_images_data = []

        for aois_index, aoi in enumerate(aois_list):
            logger.info(f"Searching AOI#: {aois_index+1}")
            tiles_gdf, num_tiles, num_captures = search_archive(aoi, start_date_str, end_date_str)
            
            presorted_fnames = []
            fnames = []

            # Handle before and after image pair
            if num_captures == 0:
                logger.info(f"No captures found for AOI: {aois_index+1}.")
                continue
            elif num_captures == 1:
                logger.warning(f"Found 1 NEW capture for AOI: {aois_index+1}.")
                # Search for images from the last month to find 'before' image
                month_ago = end_date - timedelta(days=config.DAYS_TO_SEACH_FOR_BEFORE_IMAGERY)
                month_ago_str = month_ago.strftime('%Y-%m-%d')
                tiles_gdf, num_tiles, num_captures = search_archive(aoi, month_ago_str, end_date_str)
                logger.warning(f"Found {num_captures} in past {config.DAYS_TO_SEACH_FOR_BEFORE_IMAGERY} days for AOI: {aois_index+1}.")

                if num_captures > 1:
                    # Save and animate the tiles
                    result = save_and_animate_tiles(tiles_gdf, aoi)

                    # Check for valid result before proceeding
                    if result:
                        animation_abs_path, image_filenames = result
                    else:
                        logger.info("Animation not created. Skipping...")
                        continue
                elif num_captures == 1: # Since we know there is at least 1 this should only happen when more than one are found
                    # Save and animate the tiles
                    result = save_and_animate_tiles(tiles_gdf, aoi)

                    # Check for valid result before proceeding
                    if result:
                        animation_abs_path, image_filenames = result
                        create_single_image_html(image_filenames)
                    else:
                        print("Animation not created. Skipping...")
                        continue
                    
                else:
                    print("No filenames of images created.")
            else:
                logger.warning(f"Found {num_captures} NEW captures for AOI: {aois_index+1}.")
                # Save and animate the tiles
                result = save_and_animate_tiles(tiles_gdf, aoi)

                # Check for valid result before proceeding
                if result:
                    animation_abs_path, image_filenames = result
                else:
                    print("Animation not created. Skipping...")
                    continue

            master_map = update_map_with_tiles(master_map, tiles_gdf, animation_abs_path, aoi)

            grouped_items_GPDF = group_by_capture(tiles_gdf) #returns grouped tiles by capture.            
            
            # Capture data for the email.
            if num_tiles > 0:
                for (capture_date, outcome_id), group in grouped_items_GPDF:
                    cloud_cover_mean = None
                    cloud_cover_mean = group["eo:cloud_cover"].mean()
                    center = aoi.centroid
                    lat_long = (center.y, center.x)  # Assuming 'lat' and 'lon' are keys in your AOI
                    
                    # Add the found image data to the list
                    found_images_data.append(
                        {
                            'Capture Date': capture_date,
                            'Outcome ID': outcome_id,
                            'AOI Lat_Long': lat_long,
                            'Cloud_Cover': cloud_cover_mean,
                            'Animation_Abs_Path': animation_abs_path,
                            'Country': country
                        }
                    )

        if len(found_images_data) > 0:
            email_body = format_email_body(points_filename, found_images_data)
            subject = 'New Satellogic Imagery Available'
            to_email = points_feature['properties'].get('emails', None)
            send_email(to_email, subject, email_body)
            logger.info("Sent email for captures to: to_email.")
        
        # Update the feature in the array with the modified version
        points_feature['properties']['timeOfLastSearchUtc'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        features_array[features_index] = points_feature
    
    geojson_data['features'] = features_array  # Update the 'features' field with the modified array
    with open(POINTS_FILE_PATH, 'w') as f:
        json.dump(geojson_data, f)    
    logger.warning(f"Points Search Complete For This Period, Searching Again In {config.SUBC_MON_FREQUENCY} Minutes.")
    # We have updated the points feature.  Pass back to be updated.
    return True

def main():
    now = datetime.now().strftime("%Y-%m-%dT%H%M%S")
    logging.basicConfig(filename=f"log/IndicationsAndWarnings-{now}.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Add StreamHandler to log to console as well
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.getLogger().addHandler(console)

    check_and_notify_points()
    schedule.every(config.SUBC_MON_FREQUENCY).minutes.do(check_and_notify_points)

    while True:
        schedule.run_pending()
        time.sleep(1)

        # schedule.every().day.at(query_time_utc).do(task)



if __name__ == "__main__":
    main()


