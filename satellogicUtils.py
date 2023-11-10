import os
from geopy.geocoders import Nominatim
import numpy as np
import geopandas as gpd
import pandas as pd
from pystac import ItemCollection
from pystac_client import Client, ItemSearch
import rasterio
from rasterio.merge import merge
from shapely.geometry import Polygon
from shapely.ops import transform
import matplotlib.pyplot as plt 
from PIL import Image
from datetime import datetime, timedelta
import imageio
from PIL import ImageDraw, ImageFont
import re
from geopy.distance import geodesic
import config
from concurrent.futures import ThreadPoolExecutor, as_completed
# from skimage import io, exposure, color 
from rasterio.transform import from_origin
from rasterio.warp import reproject, Resampling
from pyproj import Transformer
from pathlib import Path
from packaging import version
import logging
import requests
import shutil
import sys

# Set Global Varables
img_width, img_height = 400, 300
invalid_outcome_ids = []
max_tile_count = None
logger = logging.getLogger(__name__)

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def generate_black_tile(src, bounds):
    # Create an array of zeros (black)
    black_tile = np.zeros((src.count, src.height, src.width), dtype=src.dtypes[0])
    transform = from_origin(bounds.left, bounds.top, src.res[0], src.res[1])
    return black_tile, transform

def get_tiles_from_bounds(bounds_raster, crs, step=500, precision=1000):
    all_polygons = []
    set_of_polygons = set()
    zone = crs.to_wkt().split('"')[1][-3:][:-1]
    letter = crs.to_wkt().split('"')[1][-3:][-1]

    for x in range(int(bounds_raster.left), int(np.ceil(bounds_raster.right)), step):
        for y in range(int(bounds_raster.bottom), int(np.ceil(bounds_raster.top)), step):

            easting = int(np.floor(x/precision))
            northing = int(np.floor(y/precision))

            # Define the bounds of the cell
            left = easting * precision
            right = (easting + 1) * precision
            bottom = northing * precision
            top = (northing + 1) * precision

            polygon_list = [
                (left, top),
                (right, top),
                (right, bottom),
                (left, bottom),
                (left, top)
            ]

            if (easting, northing) not in set_of_polygons:
                set_of_polygons.add((easting, northing))
            else:
                continue

            tile_name = f"{zone}{letter}_{easting}_{northing}".replace(" ", "")

            polygon_geom = Polygon(polygon_list)
            polygon = gpd.GeoDataFrame(index=[0],
                                       data=[[tile_name, zone, letter, easting, northing, crs.to_string()]],
                                       columns=["name", "zone", "letter", "x_tile", "y_tile", "crs"],
                                       crs=crs,
                                       geometry=[polygon_geom])
            all_polygons.append(polygon)

    return gpd.GeoDataFrame(pd.concat(all_polygons, ignore_index=True), crs=crs)

def resize_img_to_array(img, img_shape=(244, 244)):
    img_array = np.array(
        img.resize(
            img_shape, 
            Image.ANTIALIAS
        )
    )
    
    return img_array

def image_grid(fn_images: list, 
               text: list = [], 
               top: int = 8, 
               per_row: int = 4):
    """
    Display a grid of images with optional annotations.
    
    Parameters:
    - fn_images (list): List of image file paths.
    - text (list): List of annotations (default is an empty list).
    - top (int): Total number of images to display (default is 8).
    - per_row (int): Number of images to display per row (default is 4).
    """

    # Iterate through the specified number of images
    for i in range(len(fn_images[:top])):

        # Create a new row of subplots for every 4th image
        if i % 4 == 0:
            _, ax = plt.subplots(1, per_row,  # 1 row, 'per_row' columns
                                  sharex='col', sharey='row',  # Share axes
                                  figsize=(24, 6))  # Figure size

        # Determine the column index for placing the image in the grid
        j = i % 4

        # Open and resize the current image
        image = Image.open(fn_images[i])
        image = resize_img_to_array(image, img_shape=(img_width, img_height))

        # Display the image in the appropriate subplot
        ax[j].imshow(image)
        ax[j].axis('off')  # Turn off the axis for a cleaner look

        # If text annotations are provided, add them below the images
        if text:
            ax[j].annotate(text[i],  # Text to annotate
                          (0, 0), (0, -32),  # Text position and offset
                          xycoords='axes fraction',  # Coordinate system for the text position
                          textcoords='offset points',  # Coordinate system for the text offset
                          va='top')  # Vertical alignment of the text


def mosaic_tiles(tiles_gdf, outfile):
    global invalid_outcome_ids
    src_files_to_mosaic = []

    for _, tile in tiles_gdf.iterrows():
        # Open the satellite image file with rasterio
        url = tile['analytic_url']
        src = rasterio.open(url)
        src_files_to_mosaic.append(src)

        outcome_id = tile['satl:outcome_id']
        product_version = tile['satl:product_version']
        if not is_version_valid(product_version) and outcome_id not in invalid_outcome_ids:
            invalid_outcome_ids.append(outcome_id)
            logger.warning(f"Tile Version Incompatible: {outcome_id}, ProdVer: {product_version}")

    mosaic, out_trans = merge(src_files_to_mosaic, indexes=[1, 2, 3])

    # Metadata for the mosaic
    meta = {
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans,
        "crs": tiles_gdf.crs,
        "count": mosaic.shape[0],
        "dtype": mosaic.dtype
    }

    with rasterio.open(outfile, "w", **meta) as dest:
        dest.write(mosaic)
    logger.warning(f"Mosaic Complete:{outfile}")


def connect_to_archive():
    if config.IS_INTERNAL_TO_SATL == True:
        logging.debug("Using Internal Archive Access.")
        archive = Client.open(config.INTERNAL_STAC_API_URL)
    else:
        API_KEY_ID = config.KEY_ID
        API_KEY_SECRET = config.KEY_SECRET
        STAC_API_URL = config.STAC_API_URL
        logger.debug("Using Credentials Archive Access")
        headers = {"authorizationToken":f"Key,Secret {API_KEY_ID},{API_KEY_SECRET}"}
        logger.debug(f"URL: {config.STAC_API_URL}")
        logger.debug(f"Header: {headers}")
        
        archive = Client.open(STAC_API_URL, headers=headers)
        response = requests.get(STAC_API_URL, headers=headers)  # include your auth headers here
        logger.debug(response.status_code)
    return archive

# Function to split the date range into two-month chunks
def date_range_chunks(start_date, end_date, chunk_size_months=1):
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    delta = timedelta(days=chunk_size_months * 30)  # Roughly two months

    while start < end:
        chunk_end = min(start + delta, end)
        yield start.isoformat(), chunk_end.isoformat()
        start = chunk_end

# Modified search function to accept start and end dates
def search_with_dates(aoi, start_date, end_date):
    try:
        # Connect To The Archive
        archive = connect_to_archive()
        logger.debug("Connected To Archive")

        if not archive:
            logger.error("Failed to connect to archive.")
            return None, 0, 0
        logger.debug(f"Start-End: {start_date}-{end_date}")
        items = archive.search(
            intersects=aoi,
            collections=["quickview-visual"],
            datetime=f"{start_date}/{end_date}",
            query={"satl:product_name": {"eq": "QUICKVIEW_VISUAL"}},
        ).item_collection()

        logger.debug(f"Search Complete for period: {start_date} to {end_date}!")

        if items is None:
            logger.debug(f"No results returned for period: {start_date} to {end_date}")
            return None

        if not isinstance(items, ItemCollection):  
            logger.error(f"Unexpected type returned: {type(items)}")
            return None

        if len(items) == 0:
            logger.debug(f"Search returned an empty collection for period: {start_date} to {end_date}")
            return None
        logger.debug(f"Num Tiles Found: {len(items)}")

        return items

    except Exception as e:
        logger.error(f"Error during search for period: {start_date} to {end_date}: {e}")
        return None

# Main function to handle multi-threading based on date ranges
def search_archive(aoi, start_date, end_date):
    # Generate date chunks
    date_chunks = list(date_range_chunks(start_date, end_date))

    # Container for all the results
    all_results = []

    # Use ThreadPoolExecutor to run searches in parallel
    with ThreadPoolExecutor(max_workers=len(date_chunks)) as executor:
        # Create a dictionary to hold futures
        future_to_date = {
            executor.submit(search_with_dates, aoi, chunk_start, chunk_end): (chunk_start, chunk_end)
            for chunk_start, chunk_end in date_chunks
        }

        # Collect the results as they complete
        for future in as_completed(future_to_date):
            try:
                result = future.result()
                if result and len(result) > 0:
                    all_results.append(result)  # Append each result assuming it found tiles.
                    logger.debug(f"Appending Tiles: {len(result)}")
            except Exception as exc:
                date_range = future_to_date[future]
                logger.error(f"Search for range {date_range} generated an exception: {exc}")

    all_gdfs = []

    for items in all_results:
        if items and len(items) > 0:
            gdf = setup_GDF(items)
            all_gdfs.append(gdf)
        else:
            # Option 1: Log the absence of data
            logger.debug("No items found for a date chunk, skipping...")

    # Combine all GeoDataFrames into one
    combined_gdf = pd.concat(all_gdfs, ignore_index=True)

    # Transform the result to a geodataframe to easily manipulate and explore the data
    # grouped = group_items_into_GPDF(combined_items)
    grouped = group_by_capture(combined_gdf)
    
    # Print the results to the log.
    num_captures = 0
    for (capture_date, outcome_id), group in grouped:
        tile_count = len(group)
        # Attempt to get the cloud cover information from the 'eo:cloud_cover' property.
        cloud_cover_mean = None
        if 'eo:cloud_cover' in group.columns:
            cloud_cover_mean = group["eo:cloud_cover"].mean()
        else:
            cloud_cover_mean = 101
            logger.info("Column 'eo:cloud_cover' doesn't exist, Setting CC to 101!")
        
        # Grab the first tile's product version
        product_version = group.iloc[0]['satl:product_version']
        
        logger.warning(f"Capture Date: {capture_date}, Outcome ID: {outcome_id}, Tile Count: {tile_count}, Cloud Cover: {cloud_cover_mean:.0f}%, Prod. Ver.: {product_version}")
        num_captures = num_captures + 1

    # Return the search results
    return combined_gdf, len(combined_gdf), num_captures

def is_version_valid(product_version):
    # Check that the version number is valid or not.
    # Should be that anything more than 1.0.0 is okay, but depends on
    # the features used.
    # Compare product_version with "1.0.0"
    # logger.warning(f"Product_Version: {version.parse(product_version)}")

    return version.parse(product_version) >= version.parse(config.MIN_PRODUCT_VERSION)

def is_group_valid(group_df):
    global max_tile_count  # Use the global max_tile_count
    global invalid_outcome_ids
    tile_count = len(group_df)

    if max_tile_count is None:
        logger.error("Rejected: max_tile_count is not set!")
        return False

    capture_date = group_df.iloc[0]['capture_date']
    # Rejection based on tile coverage
    if tile_count < max_tile_count * config.MIN_TILE_COVERAGE_PERCENT:
        logger.warning(f"Capture {capture_date} Rejected Due To Insufficient Tile Coverage: {tile_count}/{max_tile_count}")
        return False
    
    mean_cloud_cover = group_df['eo:cloud_cover'].mean()
    product_version = group_df.iloc[0]['satl:product_version']
    outcome_id = group_df.iloc[0]['satl:outcome_id']

    if not is_version_valid(product_version):
        invalid_outcome_ids.append(outcome_id)
        logger.warning(f"Capture Rejected Due To Version: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
        return False
    
    if pd.isna(mean_cloud_cover) or mean_cloud_cover > config.CLOUD_THRESHOLD:
        logger.warning(f"Capture Rejected Due To Cloud Cover: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
        return False
    
    return True

def convert_geotiff_to_png(geotiff_path):
    # Replace .TIFF with .PNG to create the new filename
    png_path = os.path.splitext(geotiff_path)[0] + '.PNG'
    
    # Open the GeoTIFF and save it as a PNG
    with Image.open(geotiff_path) as img:
        img.save(png_path)
    # Get the absolute path
    abs_png_path = os.path.abspath(png_path)
    return abs_png_path

def create_single_image_html(fnames):
    logger.info(f"FNames: {fnames}")
    if len(fnames) != 1:
        logger.error("There needs to be only one image to create HTML.")
        return False

    image_path = fnames[0]
    # Appending '_resized.tiff' to the filenames
    image_path_resized = os.path.splitext(image_path)[0] + '_resized.tiff'
    
    # Get the absolute paths
    abs_image_path_resized = os.path.abspath(image_path_resized)
    
    capture_date = extract_date(image_path).strftime("%Y%m%dT%H%M%S")
    
    # Take the geotiffs and save them as PNG to support web display.
    abs_png_before_image_path = convert_geotiff_to_png(abs_image_path_resized)

    # Convert to URI format
    uri_image_path = Path(abs_png_before_image_path).as_uri()

    html_content = f"""
        <html>
        <head>
            <title>Before and After</title>
        </head>
        <body>
            <h1>Before and After Comparison</h1>
            <div style="display: flex;">
                <div style="flex: 1;">
                    <h2>Before - {capture_date}</h2>
                    <img src="{uri_image_path}" alt="New Image Received" style="width: 100%;">
                </div>
            </div>
        </body>
        </html>
        """

    # Save to an HTML file
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    html_filename = f"images/NewCapture_{capture_date}-Generated_{now}.html"

    # Make sure the directory exists
    if not os.path.exists('images'):
        os.makedirs('images')

    with open(html_filename, "w") as f:
        f.write(html_content)

    return True

def create_before_and_after(fnames):
    if len(fnames) < 2:
        logging.error("Not enough images to create a before and after comparison.")
        return False

    before_image_path = fnames[-2]
    after_image_path = fnames[-1]

    # Appending '_resized.tiff' to the filenames
    before_image_path_resized = os.path.splitext(before_image_path)[0] + '_resized.tiff'
    after_image_path_resized = os.path.splitext(after_image_path)[0] + '_resized.tiff'

    # Get the absolute paths
    abs_before_image_path = os.path.abspath(before_image_path_resized)
    abs_after_image_path = os.path.abspath(after_image_path_resized)

    before_date = extract_date(before_image_path).strftime("%Y%m%dT%H%M%S")
    after_date = extract_date(after_image_path).strftime("%Y%m%dT%H%M%S")
    
    # Take the geotiffs and save them as PNG to support web display.
    abs_png_before_image_path = convert_geotiff_to_png(abs_before_image_path)
    abs_png_after_image_path = convert_geotiff_to_png(abs_after_image_path)

    # Convert to URI format
    uri_before_image_path = Path(abs_png_before_image_path).as_uri()
    uri_after_image_path = Path(abs_png_after_image_path).as_uri()

    html_content = f"""
        <html>
        <head>
            <title>Before and After</title>
        </head>
        <body>
            <h1>Before and After Comparison</h1>
            <div style="display: flex;">
                <div style="flex: 1;">
                    <h2>Before - {before_date}</h2>
                    <img src="{uri_before_image_path}" alt="Before" style="width: 100%;">
                </div>
                <div style="flex: 1;">
                    <h2>After - {after_date}</h2>
                    <img src="{uri_after_image_path}" alt="After" style="width: 100%;">
                </div>
            </div>
        </body>
        </html>
        """

    # Save to an HTML file
    now = datetime.now().strftime("%Y%m%dT%H%M%S")
    # before_and_after_filename = f'images/BeforeAfter_{before_date}-{after_date}-Generated-{now}.HTML' 
    before_and_after_filename = f"images/BeforeAfter_{before_date}-{after_date}-Generated_{now}.html"

    # Make sure the directory exists
    if not os.path.exists('images'):
        os.makedirs('images')

    with open(before_and_after_filename, "w") as f:
        f.write(html_content)

    return True

def save_and_animate_tiles(tiles_gdf, bbox_aoi):
    abs_output_animation_filename = None
    if tiles_gdf.empty:
        logger.warning("No items found to be animated.")
        return None

    fnames = []
    grouped = group_by_capture(tiles_gdf)

    global max_tile_count
    max_tile_count = max(len(group) for _, group in grouped)

    def process_group(capture_date, outcome_id, group_df):
        if False == is_group_valid(group_df):
            return None  # return None if the group is rejected
        logger.info(f"Spawned Mosaic Process: {capture_date}, {outcome_id}")

        # Filter rows from tiles_gdf where their 'id' is in the group_df index
        
        fname = f"CaptureDate_{capture_date.strftime('%Y%m%dT%H%M%S')}_MosaicCreated_{datetime.now().strftime('%Y%m%dT%H%M%S')}.tiff"
        full_path = os.path.join('images', fname)
        mosaic_tiles(group_df, full_path)
        return full_path  # return the filename if the group is processed

    # Use ThreadPoolExecutor to process groups in parallel
    with ThreadPoolExecutor() as executor:
        # Create a list to hold the Future objects
        futures = []

        # Iterate over each group and submit it for processing
        for (capture_date, outcome_id), group_df in grouped:
            # Submit the group to the process_group function
            future = executor.submit(process_group, capture_date, outcome_id, group_df)
            # Add the future to the list
            futures.append(future)

        # Iterate over the futures as they complete
        for future in as_completed(futures):
            # Get the result from the future
            result = future.result()
            # If there's a result, add it to the fnames list
            if result:
                fnames.append(result)

    animate_images = "y" #input("Animate stack of tiles? (y/n):") or "y"
    
    if animate_images == "y":
        try:
            logger.info("Animating Images...")
            seconds_between_frames = config.PERIOD_BETWEEN_FRAMES
            now = datetime.now().strftime("%Y%m%dT%H%M%S")
            output_animation_filename = f'images/Stack_Animation_Video_{now}.GIF' 
            
            create_animation_from_files(fnames, output_animation_filename, seconds_between_frames, bbox_aoi)
            create_before_and_after(fnames)

            # Validate that the file was actually created
            if os.path.isfile(output_animation_filename):
                abs_output_animation_filename = os.path.abspath(output_animation_filename)
                abs_output_animation_filename = abs_output_animation_filename.replace('\\', '/')
            else:
                logger.warning("Animation file was not created. Check for errors in create_animation_from_files.")
                return None
        except Exception as e:
            logger.error(f"Error occurred while creating animation: {e}")
            return None
    
    return abs_output_animation_filename, fnames 

def show_progress_bar(iteration, total, bar_length=50):
    progress = float(iteration) / float(total)
    arrow = '-' * int(round(progress * bar_length) - 1)
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write(f'\r[{arrow}{spaces}]')
    sys.stdout.flush()

def save_tiles_for_bbox(tiles_gdf, bbox_aoi):
    if tiles_gdf.empty:
        logger.warning("No items found to be animated.")
        return False
    
    grouped_items_GPDF = group_by_capture(tiles_gdf)

    # Create a global directory for all tiles
    now = datetime.now().strftime("%y-%m-%dT%H%M%S")
    directory_name = f"images/Tiles_{now}"
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)

    # for index, (capture_date, outcome_id), group in grou[]ped_items_GPDF:
    for index, ((capture_date, outcome_id), group) in enumerate(grouped_items_GPDF):
        logger.debug(f"Processing Group: {index+1}")
        logger.debug(f"Len of Group: {len(group)}")
        # subItems = ItemCollection([x for x in items if x.id in group.index])
        tile_number = 1
        
        for id, tile_gdf in group.iterrows():
            logger.debug(f"Processing Tile: {id+1}")
            # Check the tile cloud cover and reject if cloudy.
            cloud_cover = tile_gdf['eo:cloud_cover']
            logger.debug(f"Tile_GDF: {tile_gdf}")
            if cloud_cover is None:
                logger.warning("Cloud cover information missing. Skipping tile...")
                continue
            
            # Check the tile cloud cover and reject if cloudy.
            if cloud_cover > config.CLOUD_THRESHOLD:
                logger.warning(f"Tile Rejected With Cloud Cover Of: {cloud_cover:.0f}")
                continue
            url = tile_gdf['analytic_url']
            capture_date_str = capture_date.strftime("%Y-%m-%dT%H%M%SZ")
            tile_filename = os.path.join(directory_name, f"QuickView_Tile_CD_{capture_date_str}_ID_{tile_number}.tif")
            logger.debug(f"Tile_filename: {tile_filename}")
            logger.debug(f"Analytic URL: {url}")

            # Download and save the tile
            try:
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(tile_filename, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)
            except Exception as e:
                logger.error(f"Failed to save tile: {e}")
                continue

            
            tile_number += 1

        show_progress_bar(index+1, len(grouped_items_GPDF))
    show_progress_bar(len(grouped_items_GPDF), len(grouped_items_GPDF))
    return True

def extract_date(filename):
    # Split the filename into its constituent parts
    parts = filename.split(os.sep)
    
    # Assume the date is in the second part of the filename and the first part of that second part
    date_str = parts[1].split('_')[1]  
    
    # Convert the date string to a datetime object
    datetime_str = datetime.strptime(date_str, '%Y%m%dT%H%M%S')
    
    return datetime_str

def get_max_dimensions(image_filenames):
    # Find the largest image from the source images for the animation.
    max_width, max_height = 0, 0
    for image_filename in image_filenames:
        with Image.open(image_filename) as image:
            width, height = image.size
            max_width = max(max_width, width)
            max_height = max(max_height, height)
    return max_width, max_height

def get_max_dimensions_and_bounds(image_filenames):
    max_width, max_height = 0, 0
    largest_bounds = None

    for fname in image_filenames:
        with rasterio.open(fname) as src:
            width, height = src.width, src.height
            bounds = src.bounds

            max_width = max(max_width, width)
            max_height = max(max_height, height)

            if largest_bounds is None:
                largest_bounds = bounds
            else:
                largest_bounds = (
                    min(largest_bounds[0], bounds[0]),  # min left
                    min(largest_bounds[1], bounds[1]),  # min bottom
                    max(largest_bounds[2], bounds[2]),  # max right
                    max(largest_bounds[3], bounds[3])   # max top
                )
                
    return max_width, max_height, largest_bounds


# def normalize_image_color(reference_img_path, output_img_path):
#     # Read the reference image
#     reference_img = io.imread(reference_img_path)
    
#     # Enhance appearance using Contrast Stretching
#     p2, p98 = np.percentile(reference_img, (2, 98))
#     reference_img_rescale = exposure.rescale_intensity(reference_img, in_range=(p2, p98))
    
#     # Save the enhanced reference image
#     io.imsave(output_img_path, reference_img_rescale)

# def color_balance_image(source_img_path, reference_img_path, output_img_path):
#     # Read source and reference images
#     source_img = io.imread(source_img_path)
#     reference_img = io.imread(reference_img_path)
    
#     # Convert to CIELAB color space
#     source_lab = color.rgb2lab(source_img)
#     reference_lab = color.rgb2lab(reference_img)
    
#     # Perform histogram matching on the luminance channel
#     matched_l = exposure.match_histograms(source_lab[:,:,0], reference_lab[:,:,0])
    
#     # Replace the luminance channel in the source image
#     source_lab[:,:,0] = matched_l
    
#     # Convert back to RGB color space
#     matched_img = color.lab2rgb(source_lab)
    
#     # Save the matched image
#     io.imsave(output_img_path, (matched_img * 255).astype(np.uint8))

def resize_mosaics_to_largest(image_filenames):
    max_width, max_height, largest_bounds = get_max_dimensions_and_bounds(image_filenames)

    # Create a list to store the names of resized images
    new_filenames = []

    for fname in image_filenames:
        # Open the source file
        with rasterio.open(fname) as src:
            # Calculate new transform
            out_transform = rasterio.transform.from_bounds(*largest_bounds, max_width, max_height)

            # Update metadata
            out_meta = src.meta.copy()
            out_meta.update({
                "height": max_height,
                "width": max_width,
                "transform": out_transform
            })

            # Create a destination array
            dest_data = np.zeros((src.count, max_height, max_width))

            reproject(
                source=rasterio.band(src, range(1, src.count + 1)),  # All bands
                destination=dest_data,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=out_transform,
                dst_crs=src.crs,
                resampling=Resampling.cubic
            )

            # Change the filename for the new resized image
            new_fname = fname.replace(".tiff", "_resized.tiff")

            # Write out the resized image to a new file
            with rasterio.open(new_fname, 'w', **out_meta) as dest:
                dest.write(dest_data)

            # Append the new filename to our list
            new_filenames.append(new_fname)

    return new_filenames

def resize_mosaics_to_bbox(image_filenames, buffered_bounds):
    """Resize image mosaics to fit the given bounding box."""
    minx, miny, maxx, maxy = buffered_bounds
    
    # New filenames for resized images
    resized_filenames = []
    
    for fname in image_filenames:
        with rasterio.open(fname) as src:
            # Calculate new transform
            out_transform = rasterio.transform.from_bounds(minx, miny, maxx, maxy, src.width, src.height)
            
            # Update metadata
            out_meta = src.meta.copy()
            out_meta.update({
                "height": src.height,
                "width": src.width,
                "transform": out_transform
            })

            # Create a destination array
            dest_data = np.zeros((src.count, src.height, src.width))

            reproject(
                source=rasterio.band(src, range(1, src.count + 1)),
                destination=dest_data,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=out_transform,
                dst_crs=src.crs,
                resampling=Resampling.nearest
            )

            # Change the filename for the new resized image
            new_fname = fname.replace(".tiff", "_resized.tiff")

            resized_filenames.append(new_fname)
            with rasterio.open(new_fname, 'w', **out_meta) as dest:
                dest.write(dest_data)
    
    return resized_filenames


def buffer_bbox(bbox_aoi, buffer_km=0.5, crs='epsg:4326'):
    # &&&&&&This function crashes the system&&&&&&
    
    """Buffer a bounding box by a certain number of meters."""
    
    # Initialize transformer
    transformer = Transformer.from_crs(crs, "epsg:3395")  # WGS 84 / World Mercator
    
    # Transform to meters (WGS 84 / World Mercator)
    aoi_shape_meters = transform(transformer.transform, bbox_aoi)
    
    # Buffer in meters
    aoi_shape_meters = aoi_shape_meters.buffer(buffer_km * 1000)
    
    # Transform back to original CRS
    transformer_back = Transformer.from_crs("epsg:3395", crs)
    
    aoi_shape = transform(transformer_back.transform, aoi_shape_meters)
    
    return aoi_shape.bounds  

def sort_filenames(fnames):
    fnames.sort(key=extract_date)
    return fnames

def create_animation_from_files(image_filenames, output_filename, pause_duration, bbox_aoi):
    logger.info(f"Creating Animation For Filenames: {image_filenames}")
    # Sort the image filenames based on the capture date
    image_filenames.sort(key=extract_date)
    
    resized_filenames = resize_mosaics_to_largest(image_filenames)

    # Create a writer object
    duration_ms = pause_duration * 1000  # Convert seconds to milliseconds
    
    # Seek the largest dimensions
    max_width, max_height, largest_bounds = get_max_dimensions_and_bounds(resized_filenames)

    writer = imageio.get_writer(output_filename, duration=duration_ms, macro_block_size=1, loop=0)

    # Iterate through the image filenames and add them to the animation
    for index, image_filename in enumerate(resized_filenames):
        logger.info(f"Animating File: {image_filename}")

        # Load the image using PIL
        image = Image.open(image_filename)
        resized_image = image.resize((max_width, max_height), Image.LANCZOS)
        # Now we use the largest dimensions for our blank canvas
        blank_image = Image.new('RGBA', (max_width, max_height), 'black')
        
        # blank_image.paste(resized_image.convert("RGBA"), offset)
        blank_image.paste(resized_image.convert("RGBA"), (0,0))
                
        # Create a drawing context
        draw = ImageDraw.Draw(blank_image)
        
        # Define the font and size
        enlarged_font_size = 100
        font = ImageFont.truetype("fonts/tahoma.ttf", enlarged_font_size)
        
        # Extract the date from the filename and format it
        date = extract_date(image_filename).strftime('%Y-%m-%dT%H%M%S')
        
        # Extract bounds from Polygon object
        minx, miny, maxx, maxy = bbox_aoi.bounds

        # Calculate center latitude and longitude
        center_lat = (miny + maxy) / 2
        center_long = (minx + maxx) / 2

        # Create the label text with Date and Lat/Long
        label_text = f"Date: {date} | Lat: {center_lat:.4f}, Long: {center_long:.4f}"

        # Calculate the width of the text
        text_width = draw.textlength(label_text, font=font)
        
        # Define the position to center the text horizontally, near the top vertically
        position = ((max_width - text_width) / 2, 10)
        
        # Draw the text on the image in yellow
        draw.text(position, label_text, fill="yellow", font=font)

        # # Calculate the width of the text
        # text_width = draw.textlength(f"Date: {date}", font=font)
        
        # # Define the position to center the text horizontally, near the top vertically
        # position = ((max_width - text_width) / 2, 10)
        
        # # Draw the date text on the image in yellow
        # draw.text(position, f"Date: {date}", fill="yellow", font=font)
        
        # Convert blank_image to a numpy array
        image_np = np.array(blank_image)
        
        writer.append_data(image_np)
    
    # Close the writer to finalize the animation
    writer.close()
    logger.info(f"Animation saved as: {output_filename}.")
    
def get_lat_long_from_place(place):
    # The regex below matches a pattern like '-34.2355, 19.2157'
    lat_long_pattern = re.compile(r'^-?\d+\.\d+,\s*-?\d+\.\d+$')
    if lat_long_pattern.match(place):
        # If the input matches the lat-long pattern, split it into lat and long
        lat, lon = map(float, place.split(','))
        return lat, lon
    else:
        geolocator = Nominatim(user_agent="ZXCVBGFDSAaasdfg12413415")
        location = geolocator.geocode(place)
        lat, lon = location.latitude, location.longitude

    return lat, lon

def flip_coordinates(polygon):
    def flip(coord_pair):
        return [coord_pair[1], coord_pair[0]]

    flipped_polygon = []
    for exterior_or_interior_ring in polygon['coordinates']:
        flipped_ring = list(map(flip, exterior_or_interior_ring))
        flipped_polygon.append(flipped_ring)

    return {
        'type': 'Polygon',
        'coordinates': flipped_polygon
    }

def km_to_deg(km, latitude):
    # Determines the number of degrees per km at a given latitude
    coords_from = (latitude, 0)
    coords_to = (latitude, geodesic(kilometers=km).destination(coords_from, 90)[1])  # 90 degrees for eastward distance
    return coords_to[1] - coords_from[1]  # subtract the longitudes to get the degrees per km

def print_invalid_outcome_ids():
    # Generate a timestamp for the filename
    timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    filename = f'invalid_outcome_ids/invalid_outcome_ids_{timestamp}.txt'
    
    # At the end of your animation function, or wherever you deem appropriate, append them to the file.
    global invalid_outcome_ids
    unique_ids = set(invalid_outcome_ids)
    with open(filename, 'a') as f:  # 'a' for append mode
        for id_ in unique_ids:
            f.write(f"{id_}\n")

    return True

def setup_GDF(items):
    # Check if items is None or empty
    if items is None or len(items) == 0:
        logger.error("Error: Trying To Group Empty Items!")
        return False
    
    # Iterate through items to get the first item to confirm things are okay.
    first_item = next(iter(items), None)
    if first_item is None:
        logger.error("Error: No items available to setup GeoDataFrame!")
        return False
    
    gdf = gpd.GeoDataFrame.from_features(items.to_dict(), crs=f"epsg:{first_item.properties['proj:epsg']}")
    gdf['id'] = [x.id for x in items]
    gdf.set_index('id', inplace=True)
    gdf['capture_date'] = [gpd.pd.to_datetime(f"{x[0]}T{x[1]}") for x in gdf.index.str.split("_")]
    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.buffer(0))  # this is because sometimes footprints are not valid polygons, it will be fixed.
    gdf['data_age'] = (datetime.utcnow() - gdf['capture_date']).dt.days  # Using utcnow
    # print(f"GDF Cols: {gdf.columns}")
    
    # Count the number of tiles in each 'grid:code'
    tile_counts = gdf['grid:code'].value_counts().reset_index()
    tile_counts.columns = ['grid:code', 'image_count']

    # Join this back to the original GeoDataFrame
    gdf = pd.merge(gdf, tile_counts, on='grid:code', how='left')
    
    # # Sort by age so that youngest tiles are last (and thus displayed on top)
    # gdf.sort_values(by='data_age', ascending=False, inplace=True)

    # Create lists to hold the asset URLs
    preview_urls = []
    thumbnail_urls = []
    analytic_urls = []
    outcome_ids = []
    valid_pixel_percents = []

    # Loop through items to collect asset URLs
    for item in items:
        preview_urls.append(item.assets["preview"].href)
        thumbnail_urls.append(item.assets["thumbnail"].href)
        analytic_urls.append(item.assets["analytic"].href)
        outcome_ids.append(item.properties['satl:outcome_id'])
        valid_pixel_percents.append(item.properties['satl:valid_pixel'])

    # Add these lists as new columns in the GDF
    gdf['preview_url'] = preview_urls
    gdf['thumbnail_url'] = thumbnail_urls
    gdf['analytic_url'] = analytic_urls
    gdf['outcome_id'] = outcome_ids
    gdf['valid_pixel_percent'] = valid_pixel_percents
    
    return gdf

def group_by_capture(gdf):
    # Grouping the data
    grouped = gdf.groupby([gpd.pd.Grouper(key="capture_date", freq="S"), "satl:outcome_id"])
    return grouped # A GeoPanadasDF

def group_items_into_GPDF(items):
    if not items or len(items) == 0:
        logger.error("Error: Trying To Group Empty Items!")
        return False

    # Setup the GDF
    gdf = setup_GDF(items)

    # Grouping the data
    grouped = group_by_capture(gdf)
    return grouped # A GeoPanadasDF

def create_cloud_free_basemap(tiles_gdf):
    if tiles_gdf.empty:
        logging.warning("No Tiles Found")
        return None

    # Filter by cloud coverage and valid pixel percentage
    cloud_filtered_tiles_gdf = tiles_gdf[(tiles_gdf['eo:cloud_cover'] <= config.CLOUD_THRESHOLD) & 
                                         (tiles_gdf['valid_pixel_percent'] >= config.VALID_PIXEL_PERCENT_FOR_BASEMAP)]

    #
    # Sort by capture date
    cloud_filtered_tiles_gdf.sort_values('capture_date', ascending=False, inplace=True)
    
    
    # Group by grid cell and take the first (most recent) record
    most_recent_cloud_free_tiles = cloud_filtered_tiles_gdf.groupby('grid:code').first().reset_index()

    return most_recent_cloud_free_tiles 