# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.
#
# Functions:
#   save_and_animate_tiles
#   age_heatmap
#   count_heatmap
#   cloud_heatmap
#   update_map_with_tiles
#   create_folium_map
#   create_choropleth_map
#   filter_tiles
#   filter_and_sort_tiles
#   create_folium_basemap

from typing import Tuple, Dict, Optional, List, Type
import os
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.merge import merge
import plotly.express as px
from shapely import Point
from shapely.ops import unary_union
from shapely.geometry import shape, box, Polygon
from geopy.distance import distance
from PIL import Image
from datetime import datetime
import imageio
from PIL import ImageDraw, ImageFont
from concurrent.futures import ThreadPoolExecutor, as_completed
from rasterio.warp import reproject, Resampling
from pathlib import Path
from packaging import version
import logging
import plotly.graph_objs as go
import branca.colormap as cm
import folium
from folium import raster_layers

logger = logging.getLogger(__name__)

class Visualizer:
    def __init__(self):
        # Set Defaults
        self.period_between_frames = 2
        self.min_product_version = "1.0.0"
        self.min_tile_coverage_percent = 0.01
        self.valid_pixel_percent_for_basemap = 100
        self.cloud_threshold = 30
        self._param = None

    @property
    def param(self):
        return self._param

    @param.setter
    def param(self, value):
        self._param = value

    def animate_tile_stack(self, tiles_gdf, bbox_aoi, font=None):
        abs_output_animation_filename = None
        if tiles_gdf.empty:
            logger.warning("No items found to be animated.")
            return None

        fnames = []
        grouped = self._group_by_capture(tiles_gdf)

        # if not font is specified, use default font
        if font is None:
            font = ImageFont.load_default()

        global max_tile_count
        max_tile_count = max(len(group) for _, group in grouped)

        # Use ThreadPoolExecutor to process groups in parallel
        with ThreadPoolExecutor() as executor:
            # Create a list to hold the Future objects
            futures = []

            # Iterate over each group and submit it for processing
            for (capture_date, outcome_id), group_df in grouped:
                # Submit the group to the process_group function
                future = executor.submit(self._process_group, capture_date, outcome_id, group_df)
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
                seconds_between_frames = self.period_between_frames
                now = datetime.now().strftime("%Y%m%dT%H%M%S")
                output_animation_filename = f'images/Stack_Animation_Video_{now}.GIF'

                self._create_animation_from_files(fnames, output_animation_filename, seconds_between_frames, bbox_aoi, font)
                # create_before_and_after(fnames)

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

    def _process_group(self, capture_date, outcome_id, group_df):
        if False == self._is_group_valid(group_df):
            return None  # return None if the group is rejected
        logger.info(f"Spawned Mosaic Process: {capture_date}, {outcome_id}")

        fname = f"CaptureDate_{capture_date.strftime('%Y%m%dT%H%M%S')}_MosaicCreated_{datetime.now().strftime('%Y%m%dT%H%M%S')}.tiff"
        full_path = os.path.join('images', fname)
        self._mosaic_tiles(group_df, full_path)
        return full_path  # return the filename if the group is processed


    def age_heatmap(self, tiles_gdf: Dict, out_filename: str = None) -> folium.Map:
        """Creates a heat map based on age of data, using a linear color map."""

        # Sort the GeoDataFrame based on data_age, so that less old squares are on top
        tiles_gdf = tiles_gdf.sort_values(by='data_age', ascending=False)

        # Determine the center of your data to set the initial view of the map
        center = tiles_gdf.geometry.unary_union.centroid
        start_coord = (center.y, center.x)

        # Determine the data range for color normalization
        data_age_min, data_age_max = tiles_gdf['data_age'].min(), tiles_gdf['data_age'].max()
        # data_age_min = 0
        # data_age_max = 30 # Basically scale it to a month.
        colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[data_age_min, data_age_max],
                                    vmin=data_age_min, vmax=data_age_max)

        # Create the folium map
        m = folium.Map(location=start_coord, zoom_start=8)

        for idx, row in tiles_gdf.iterrows():
            # Scaling opacity: younger squares more opaque (0.8), older squares less opaque (0.4)
            opacity_scaled = 0.8 - \
                ((row['data_age'] - data_age_min) /
                (data_age_max - data_age_min)) * 0.4

            tooltip_text = f"Age: {row['data_age']}"
            tooltip = folium.Tooltip(tooltip_text)
            folium.Polygon(
                locations=[(y, x) for x, y in zip(
                    row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
                color=colormap(row['data_age']),
                fill=True,
                fill_color=colormap(row['data_age']),
                fill_opacity=opacity_scaled,
                opacity=opacity_scaled,
                tooltip=tooltip
            ).add_to(m)

        m.add_child(colormap)  # Add the color map legend

        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/ImageAge_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
        m.save(out_filename)  # Save to an HTML file

        return m


    def count_heatmap(self, tiles_gdf: Dict, out_filename: str = None) -> folium.Map:
        """Creates a heat map based on quantity of available data using a linear color map."""

        # Keep only the latest tile for each grid code.
        # Since they are sorted by age with the youngest last, we can drop duplicates except for the last one.
        # All tiles in a gridcode have the same tilecount field.
        tiles_gdf = tiles_gdf.drop_duplicates(subset='grid:code', keep='last')

        # Sort by age so that youngest tiles are last (and thus displayed on top)
        tiles_gdf = tiles_gdf.sort_values(by='data_age', ascending=False)

        # Determine the center of your data to set the initial view of the map
        center = tiles_gdf.geometry.unary_union.centroid
        start_coord = (center.y, center.x)

        # Determine the data range for color normalization
        count_min, count_max = tiles_gdf['image_count'].min(), tiles_gdf['image_count'].max()
        colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[count_min, count_max],
                                vmin=count_min, vmax=count_max)

        logging.info(f"ImageCountMin: {tiles_gdf['image_count'].min()}, Max: {tiles_gdf['image_count'].max()}")

        # Create the folium map
        m = folium.Map(location=start_coord, zoom_start=8, tiles='cartodbdark_matter')

        # Add polygons to the map
        for idx, row in tiles_gdf.iterrows():
            tooltip_text = f"Image Count: {row['image_count']}"
            tooltip = folium.Tooltip(tooltip_text)

            folium.Polygon(
                locations=[(y, x) for x, y in zip(row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
                color=colormap(row['image_count']),
                fill=True,
                fill_color=colormap(row['image_count']),
                fill_opacity=0.7,  # Feel free to scale this as needed
                opacity=0.7,
                tooltip=tooltip
            ).add_to(m)

        m.add_child(colormap)  # Add the color map legend
        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/ImageCount_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"
        m.save(out_filename)  # Save to an HTML file

        return m

    def cloud_heatmap(self, tiles_gdf: Dict, existing_fig: go.Figure = None, out_filename: str = None) -> go.Figure:
        """Creates a heat map based on cloud coverage in available data using a linear color map."""

        if tiles_gdf.empty:
            logger.warning("No items found.")
            return None  # or however you want to handle an empty response

        # Reset index to have 'id' as a column for px.choropleth_mapbox
        tiles_gdf.reset_index(inplace=True)

        # Calculate the total bounds of the GeoDataFrame
        minx, miny, maxx, maxy = tiles_gdf.total_bounds

        # Calculate the midpoint of the bounds
        center_x = (maxx + minx) / 2
        center_y = (maxy + miny) / 2

        # Create a Point instance for the approximate centroid
        centroid = Point(center_x, center_y)

        # Filter tiles by cloud cover
        cloud_filtered_tiles_gdf = tiles_gdf[tiles_gdf['eo:cloud_cover']
                                            <= self.cloud_threshold].copy()

        # Rest of your code remains the same
        cloud_filtered_tiles_gdf.sort_values(by='data_age', ascending=True, inplace=True)

        cloud_filtered_tiles_gdf.drop_duplicates(subset='grid:code', keep='first', inplace=True)

        # Create figure if not provided
        if existing_fig is None:
            fig = px.choropleth_mapbox(
                cloud_filtered_tiles_gdf,
                geojson=cloud_filtered_tiles_gdf.geometry.__geo_interface__,
                locations=cloud_filtered_tiles_gdf.index,
                color="eo:cloud_cover",
                hover_data=["eo:cloud_cover"],
                center={'lat': centroid.y, 'lon': centroid.x},
                zoom=8
            )
            fig.update_traces(marker_line_width=0)
            fig.update_layout(
                width=1200,
                height=700,
                mapbox_style="carto-positron"
            )
        else:
            fig = existing_fig
            new_trace = go.Choroplethmapbox(
                geojson=cloud_filtered_tiles_gdf.geometry.__geo_interface__,
                locations=cloud_filtered_tiles_gdf.index,
                z=cloud_filtered_tiles_gdf['eo:cloud_cover']
            )
            fig.add_trace(new_trace)

        now = datetime.now()
        if out_filename is None:
            out_filename = f"maps/CloudCover_Heatmap_{now.strftime('%Y-%m-%d_%H-%M-%S')}.html"

        fig.write_html(out_filename)
        return fig

    def update_map_with_tiles(  self,
                                folium_map_obj: folium.Map,
                                tiles_gdf: gpd.GeoDataFrame,
                                animation_filename: str,
                                aoi_bbox: Polygon) -> folium.Map:
        """Update a map with new folium polygon objects."""

        if tiles_gdf.empty:
            print("No items found.")
            return None  # or however you want to handle an empty response

        grouped = self._group_by_capture(tiles_gdf)
        # Iterating through grouped data
        for (capture_date, cloud_cover), group in grouped:
            # Iterate through each geometry in the group
            for geometry in group['geometry']:
                if geometry.geom_type == 'Polygon':
                    coords = [[lat, lon] for lon, lat in list(geometry.exterior.coords)]
                    folium.Polygon(
                        locations=coords,
                        tooltip=f"CD: {capture_date}_CC: {cloud_cover}",
                        color='red',
                        fill=True,
                        fill_color='red',
                        fill_opacity=0.01
                    ).add_to(folium_map_obj)
                else:
                    print(f'Unsupported geometry type: {geometry.geom_type}')
                    return False

        # Calculate centroid of the bbox
        min_lon, min_lat, max_lon, max_lat = aoi_bbox.bounds
        centroid_lon = (min_lon + max_lon) / 2
        centroid_lat = (min_lat + max_lat) / 2
        # tooltip_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
        popup_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
        folium.Marker([centroid_lat, centroid_lon], popup_html, parse_html=True).add_to(folium_map_obj)

        return folium_map_obj


    def create_folium_map(
            self,
            points: List[Point],
            aois: List[Polygon]
        ) -> folium.Map:
        """Create a folium map and add all markers and AOIs."""

        if not points:
            raise ValueError("Points list is empty")

        # Set initial location using the first point's coordinates
        initial_lon, initial_lat = points[0].x, points[0].y
        master_map = folium.Map(location=[initial_lat, initial_lon], zoom_start=8)

        for aoi in aois:
            # Create a folium Polygon from AOI and add it to the map
            folium.Polygon(
                locations=[(x, y) for x, y in aoi.exterior.coords],
                tooltip="Search Bounding Box"
            ).add_to(master_map)

        return master_map

    def create_choropleth_map(self, aois: List[Polygon]) -> go.Figure:
        """Initialize the 'master' Plotly figure."""
        master_fig = go.Figure()
        all_aoi_shapes = []  # List to keep track of all AOI shapes

        for aoi in aois:
            # Extract centroid coordinates and create a DataFrame
            centroid = aoi.centroid
            df = pd.DataFrame({'lat': [centroid.y], 'lon': [centroid.x]})

            # Create Plotly figure for each AOI
            fig = px.scatter_mapbox(df,
                                    lat='lat',
                                    lon='lon',
                                    mapbox_style="carto-positron",
                                    zoom=8)

            all_aoi_shapes.append(shape(aoi))

            # Extract the traces from the new figure and add them to the 'master' figure
            for trace in fig.data:
                master_fig.add_trace(trace)

        # Calculate the 'global' bounding box if AOIs are present
        if all_aoi_shapes:
            global_bbox = unary_union(all_aoi_shapes).bounds
            minx, miny, maxx, maxy = global_bbox
            zoom_level = self._estimate_zoom_level(minx, miny, maxx, maxy) - 2

            # Update the layout of the 'master' figure
            master_fig.update_layout(
                mapbox={
                    "style": 'carto-positron',
                    "zoom": zoom_level,
                    "center": {
                        "lat": (miny + maxy) / 2,
                        "lon": (minx + maxx) / 2,
                    },
                }
            )

        return master_fig

    def _estimate_zoom_level(self, minx, miny, maxx, maxy):
        """Calculate the geographic extent."""

        width = maxx - minx
        height = maxy - miny

        # get the larger of the two dimensions
        max_dim = max(width, height)
        # print(f"Max_Dim:{max_dim}")
        # estimate zoom level based on max dimension
        # these thresholds are arbitrary and might need adjustment
        if max_dim > 10:
            zoom_level = 6
        elif max_dim > 5:
            zoom_level = 7
        elif max_dim > 2:
            zoom_level = 8
        elif max_dim > 1:
            zoom_level = 9
        elif max_dim > 0.5:
            zoom_level = 10
        elif max_dim > 0.25:
            zoom_level = 11
        elif max_dim > 0.125:
            zoom_level = 12
        elif max_dim > 0.0625:
            zoom_level = 13
        else:
            zoom_level = 14

        # print(f"Zoom: {zoom_level}")
        return zoom_level

    def _mosaic_tiles(self, tiles_gdf, outfile):
        src_files_to_mosaic = []

        for _, tile in tiles_gdf.iterrows():
            # Open the satellite image file with rasterio
            url = tile['analytic_url']
            src = rasterio.open(url)
            src_files_to_mosaic.append(src)

            outcome_id = tile['satl:outcome_id']
            product_version = tile['satl:product_version']
            if not self._is_version_valid(product_version):
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


    def _is_version_valid(self, product_version):
        # Check that the version number is valid or not.
        return version.parse(product_version) >= version.parse(self.min_product_version)

    def _is_group_valid(self, group_df):
        global max_tile_count  # Use the global max_tile_count
        global invalid_outcome_ids
        tile_count = len(group_df)

        if max_tile_count is None:
            logger.error("Rejected: max_tile_count is not set!")
            return False

        capture_date = group_df.iloc[0]['capture_date']
        # Rejection based on tile coverage
        if tile_count < max_tile_count * self.min_tile_coverage_percent:
            logger.warning(f"Capture {capture_date} Rejected Due To Insufficient Tile Coverage: {tile_count}/{max_tile_count}")
            return False

        mean_cloud_cover = group_df['eo:cloud_cover'].mean()
        product_version = group_df.iloc[0]['satl:product_version']
        outcome_id = group_df.iloc[0]['satl:outcome_id']

        if not self._is_version_valid(product_version):
            invalid_outcome_ids.append(outcome_id)
            logger.warning(f"Capture Rejected Due To Version: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
            return False

        if pd.isna(mean_cloud_cover) or mean_cloud_cover > self.cloud_threshold:
            logger.warning(f"Capture Rejected Due To Cloud Cover: Product_Version: {product_version}, Cloud: {mean_cloud_cover:.0f}%, OutcomeId: {outcome_id}")
            return False

        return True

    def _ensure_dir(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def _get_max_dimensions_and_bounds(self, image_filenames):
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

    def _resize_mosaics_to_largest(self, image_filenames):
        max_width, max_height, largest_bounds = self._get_max_dimensions_and_bounds(image_filenames)

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

    def _create_animation_from_files(self, image_filenames, output_filename, pause_duration, bbox_aoi, font):
        logger.info(f"Creating Animation For Filenames: {image_filenames}")
        # Sort the image filenames based on the capture date
        image_filenames.sort(key=self._extract_date)

        resized_filenames = self._resize_mosaics_to_largest(image_filenames)

        # Create a writer object
        duration_ms = pause_duration * 1000  # Convert seconds to milliseconds

        # Seek the largest dimensions
        max_width, max_height, largest_bounds = self._get_max_dimensions_and_bounds(resized_filenames)

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

            # Extract the date from the filename and format it
            date = self._extract_date(image_filename).strftime('%Y-%m-%dT%H%M%S')

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

            # Convert blank_image to a numpy array
            image_np = np.array(blank_image)

            writer.append_data(image_np)

        # Close the writer to finalize the animation
        writer.close()
        logger.info(f"Animation saved as: {output_filename}.")

    def _extract_date(self, filename):
        # Split the filename into its constituent parts
        parts = filename.split(os.sep)

        # Assume the date is in the second part of the filename and the first part of that second part
        date_str = parts[1].split('_')[1]

        # Convert the date string to a datetime object
        datetime_str = datetime.strptime(date_str, '%Y%m%dT%H%M%S')

        return datetime_str

    def filter_tiles(self, tiles_gdf, cloud_cover=None, valid_pixels_perc=None):
        """Uses the configuration value for cloud_threshold and valid_pixel_percent
           Unless overloaded by the calling parameters."""
        if tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        if cloud_cover is None:
            cloud_cover = self.cloud_threshold

        if valid_pixels_perc is None:
            valid_pixels_perc = self.valid_pixel_percent_for_basemap

        # Filter by cloud coverage and valid pixel percentage
        filtered_tiles_gdf = tiles_gdf[(tiles_gdf['eo:cloud_cover'] <= cloud_cover) &
                                            (tiles_gdf['valid_pixel_percent'] >= valid_pixels_perc)].copy()

        return filtered_tiles_gdf


    def filter_and_sort_tiles(self, tiles_gdf, cloud_cover=None, valid_pixels_perc=None):
        """Filters tiles based on cloud cover and valid pixel percent and then sorts the tiles.
           Then it keeps only the latest tile for use in heatmaps."""
        if tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        filtered_tiles_gdf = self.filter_tiles(tiles_gdf, cloud_cover, valid_pixels_perc)

        # Sort by capture date
        filtered_tiles_gdf.sort_values('capture_date', ascending=False, inplace=True)


        # Group by grid cell and take the first (most recent) record
        most_recent_cloud_free_tiles = filtered_tiles_gdf.groupby('grid:code').first().reset_index()

        return most_recent_cloud_free_tiles

    def create_folium_basemap(self, capture_grouped_tiles_gdf: Dict) -> folium.Map:
        """Create folium basemap as starting point for further updates with heatmaps."""

        if capture_grouped_tiles_gdf.empty:
            logging.warning("No Tiles Found")
            return None

        # Create a folium map
        center = capture_grouped_tiles_gdf.geometry.unary_union.centroid
        folium_map = folium.Map(location=[center.y, center.x], zoom_start=8)

        for idx, row in capture_grouped_tiles_gdf.iterrows():
            coords = [(y, x) for x, y in zip(*row.geometry.exterior.coords.xy)]
            # Create a tooltip using capture date and outcome_id
            tooltip_text = f"{row['capture_date'].strftime('%Y-%m-%dT%H%M%SZ')}, {row['outcome_id']}"
            tooltip = folium.Tooltip(tooltip_text)
            folium.Polygon(coords, color='blue', weight=1, tooltip=tooltip).add_to(folium_map)

            # Adding image overlay
            bounds = [list(row.geometry.bounds[1::-1]), list(row.geometry.bounds[3:1:-1])]

            image_url = row["thumbnail_url"]
            # logger.debug(f"Thumbnail image_url: {image_url}")
            raster_layers.ImageOverlay(image_url, bounds=bounds).add_to(folium_map)

        return folium_map

    def _group_by_capture(self, gdf):
        # Grouping the data
        grouped = gdf.groupby([gpd.pd.Grouper(key="capture_date", freq="S"), "satl:outcome_id"])
        return grouped # Returns > ((capture_date, outcome_id), GeoPanadasDF)
