# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

import folium
from folium import raster_layers
import plotly.graph_objs as go
import plotly.express as px
import branca.colormap as cm
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape, box
from geopy.distance import distance
from shapely.ops import unary_union
from datetime import datetime
import logging
from satellogicUtils import group_by_capture

logger = logging.getLogger(__name__)

def estimate_zoom_level(minx, miny, maxx, maxy):
    # calculate the geographic extent
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

def create_bounding_box(center_lat, center_lon, width_km=3):
    # Calculate the height based on the width to maintain a 16:9 aspect ratio
    height_km = width_km * 9 / 16

    # Calculate the deltas in km for each direction
    north_point = distance(kilometers=height_km/2).destination(point=(center_lat, center_lon), bearing=0)
    south_point = distance(kilometers=height_km/2).destination(point=(center_lat, center_lon), bearing=180)
    east_point = distance(kilometers=width_km/2).destination(point=(center_lat, center_lon), bearing=90)
    west_point = distance(kilometers=width_km/2).destination(point=(center_lat, center_lon), bearing=270)

    # Extract the latitude and longitude from each point
    north_lat, _ = north_point.latitude, north_point.longitude
    south_lat, _ = south_point.latitude, south_point.longitude
    _, east_lon = east_point.latitude, east_point.longitude
    _, west_lon = west_point.latitude, west_point.longitude

    # Create the bounding box
    bbox = box(west_lon, south_lat, east_lon, north_lat)
    return bbox

def create_bounding_box_choropleth(lat, lon, width=3):
    # Create Bbox
    bbox = create_bounding_box(lat, lon, width)

    # Create a DataFrame for Plotly Express
    df = pd.DataFrame({'lat': [lat], 'lon': [lon], 'geometry': [bbox]})
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # Create Plotly figure
    fig = px.scatter_mapbox(df, 
                            lat='lat', 
                            lon='lon', 
                            mapbox_style="carto-positron",
                            zoom=10)
    
    # You could add bounding box as a choropleth layer if you like.
    # For now, just returning the center marker and the GeoJSON-like object.

    return bbox.__geo_interface__, fig

def create_map(lat, lon, bbox):
    # Extracting coordinates and transforming to the format folium expects
    jsonCoords = bbox.__geo_interface__
    coords = jsonCoords['coordinates'][0]
    folium_coords = [list(coord)[::-1] for coord in coords]  # Flip lon and lat for each coordinate

    # print(f"Folium BBox Coords: {folium_coords}")
    # Get the bounds of the AOI polygon
    polygon_shape = shape(jsonCoords)
    minx, miny, maxx, maxy = polygon_shape.bounds
    zoom_level = estimate_zoom_level(minx, miny, maxx, maxy)
    # print(zoom_level)
    m_obj = folium.Map(location=[lat, lon], zoom_start=zoom_level)
    
    folium.Polygon(folium_coords, tooltip="Search Bounding Box").add_to(m_obj)  # Updated line
  
    return m_obj

def create_choropleth_map(lat, lon, bbox_geojson, fig=None):
    if fig is None:
        # Initialize a figure if it's not passed in
        fig = px.choropleth_mapbox(zoom=10, center=dict(lat=lat, lon=lon))
        
    # Adding the bounding box
    coords = bbox_geojson['coordinates'][0]
    fig.add_shape(
        go.layout.Shape(
            type="polygon",
            coordinates=coords,
            line=dict(width=2),
        ),
        row=1,
        col=1
    )
    
    # Adding the marker for the center point
    fig.add_trace(
        go.Scattermapbox(
            lat=[lat],
            lon=[lon],
            mode='markers',
            marker=go.scattermapbox.Marker(size=14, color='red')
        )
    )

    # General update to layout
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=9,
        mapbox_center={"lat": lat, "lon": lon}
    )
    
    return fig

def update_map_with_tiles(folium_map_obj, tiles_gdf, animation_filename, aoi_bbox):
    if tiles_gdf.empty:
        print("No items found.")
        return None  # or however you want to handle an empty response

    grouped = group_by_capture(tiles_gdf)
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
                exit()        

    # Calculate centroid of the bbox
    min_lon, min_lat, max_lon, max_lat = aoi_bbox.bounds
    centroid_lon = (min_lon + max_lon) / 2
    centroid_lat = (min_lat + max_lat) / 2    
    # tooltip_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
    popup_html = f'<a href="file:///{animation_filename}" target="_blank">Open Animation</a>'
    folium.Marker([centroid_lat, centroid_lon], popup_html, parse_html=True).add_to(folium_map_obj)
    return folium_map_obj

def process_multiple_points_to_bboxs(points, width):
    master_map = folium.Map(location=[points[0]['lat'], points[0]['lon']], zoom_start=13)  # Set initial location
    aois_list = []
    for index, point in enumerate(points):  # Use enumerate to get the index and point
        lat, lon = point['lat'], point['lon']
        aoi = create_bounding_box(lat, lon, width)
        folium_map_search = create_map(lat, lon, aoi)
        # Add the bounding box polygon to the list of search areas.
        aois_list.append(aoi)  # Use append to add the aoi to the list
        for feature in folium_map_search._children.values():
            master_map.add_child(feature)  # Add each feature from the individual map to the master map
    return master_map, aois_list  # Return both the master map and the list of AOIs

def process_multiple_points_choropleth(points, width):
    # Initialize the "master" Plotly figure
    master_fig = go.Figure()

    aois_list = []
    all_aoi_shapes = []  # List to keep track of all AOI shapes

    for point in points:
        lat, lon = point['lat'], point['lon']
        aoi, fig = create_bounding_box_choropleth(lat, lon, width)

        # Add AOI to the list
        aois_list.append(aoi)
        all_aoi_shapes.append(shape(aoi))

        # Extract the traces from the new figure and add them to the "master" figure
        for trace in fig.data:
            master_fig.add_trace(trace)
    
    # Calculate the "global" bounding box
    global_bbox = unary_union(all_aoi_shapes).bounds
    minx, miny, maxx, maxy = global_bbox
    zoom_level = estimate_zoom_level(minx, miny, maxx, maxy)
    zoom_level = zoom_level - 2

    # print(f"Zoom: {zoom_level}")
    # print(f"Min xy, Max xy: {minx}:{miny},{maxx}:{maxy}")
    # Update the layout of the "master" figure
    master_fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            zoom=zoom_level,
            center=dict(lat=points[0]['lat'], lon=points[0]['lon']),
        )
    )
    
    return master_fig, aois_list



def create_heatmap_for_age(aggregated_gdf):
    # Sort the GeoDataFrame based on data_age, so that less old squares are on top
    aggregated_gdf = aggregated_gdf.sort_values(by='data_age', ascending=False)
    
    # Determine the center of your data to set the initial view of the map
    center = aggregated_gdf.geometry.unary_union.centroid
    start_coord = (center.y, center.x)

    # Determine the data range for color normalization
    data_age_min, data_age_max = aggregated_gdf['data_age'].min(), aggregated_gdf['data_age'].max()
    data_age_min = 0
    data_age_max = 30 # Basically scale it to a month.
    colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[data_age_min, data_age_max],
                                 vmin=data_age_min, vmax=data_age_max)

    # Create the folium map
    m = folium.Map(location=start_coord, zoom_start=10)

    # Iterate through the GeoDataFrame and add each geometry as a folium Polygon
    # for idx, row in aggregated_gdf.iterrows():
    #     polygon = folium.Polygon(locations=[(y, x) for x, y in zip(row.geometry.exterior.xy[0],
    #                                                                row.geometry.exterior.xy[1])],
    #                              color=colormap(row['data_age']),
    #                              fill=True).add_to(m)
    for idx, row in aggregated_gdf.iterrows():
            # Scaling opacity: younger squares more opaque (0.8), older squares less opaque (0.4)
            opacity_scaled = 0.8 - ((row['data_age'] - data_age_min) / (data_age_max - data_age_min)) * 0.4
    
            tooltip_text = f"Age: {row['data_age']}"
            tooltip = folium.Tooltip(tooltip_text)
            polygon = folium.Polygon(
                locations=[(y, x) for x, y in zip(row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
                color=colormap(row['data_age']),
                fill=True,
                fill_color=colormap(row['data_age']),  # Fill color specified here
                fill_opacity=opacity_scaled,  # Adjust fill opacity here
                opacity=opacity_scaled,  # Adjust border opacity here
                tooltip=tooltip # Add tooltip of age of data.
            ).add_to(m)

    m.add_child(colormap)  # Add the color map legend
    now = datetime.now().strftime("%h-%m-%dT%H%M%SZ")
    filename = f'maps/Heatmap_Image_Age_{now}.html'
    m.save(filename)  # Save to an HTML file
    logger.warning(filename)
    return m

def create_heatmap_for_image_count(aggregated_gdf):
    # # Sort the GeoDataFrame based on image_count
    # aggregated_gdf = aggregated_gdf.sort_values(by='image_count', ascending=True)
    
    # Check data as it comes in
    # Print the DataFrame columns
    print("DataFrame Columns:", aggregated_gdf.columns.tolist())

    # Print the first row of the DataFrame
    if not aggregated_gdf.empty:
        print("First Row of Data:", aggregated_gdf.iloc[0].to_dict())
    else:
        print("The DataFrame is empty.")


    # Keep only the latest tile for each grid code.
    # Since they are sorted by age with the youngest last, we can drop duplicates except for the last one.
    # All tiles in a gridcode have the same tilecount field.
    aggregated_gdf = aggregated_gdf.drop_duplicates(subset='grid:code', keep='last')

    # Determine the center of your data to set the initial view of the map
    center = aggregated_gdf.geometry.unary_union.centroid
    start_coord = (center.y, center.x)

    # Determine the data range for color normalization
    count_min, count_max = aggregated_gdf['image_count'].min(), aggregated_gdf['image_count'].max()
    colormap = cm.LinearColormap(colors=['#90EE90', '#FF6F61'], index=[count_min, count_max],
                              vmin=count_min, vmax=count_max)

    logging.warning(f"ImageCountMin: {aggregated_gdf['image_count'].min()}, Max: {aggregated_gdf['image_count'].max()}")

    # Create the folium map
    m = folium.Map(location=start_coord, zoom_start=10, tiles='cartodbdark_matter')

    # Add polygons to the map
    for idx, row in aggregated_gdf.iterrows():
        tooltip_text = f"Image Count: {row['image_count']}"
        tooltip = folium.Tooltip(tooltip_text)
        # logging.warning(f"ToolTip: {tooltip_text}")
        
        polygon = folium.Polygon(
            locations=[(y, x) for x, y in zip(row.geometry.exterior.xy[0], row.geometry.exterior.xy[1])],
            color=colormap(row['image_count']),
            fill=True,
            fill_color=colormap(row['image_count']),
            fill_opacity=0.7,  # Feel free to scale this as needed
            opacity=0.7,
            tooltip=tooltip
        ).add_to(m)

    m.add_child(colormap)  # Add the color map legend
    now = datetime.now().strftime("%h-%m-%dT%H%M%SZ")
    filename = f'maps/Heatmap_Image_Count_{now}.html'
    m.save(filename)  # Save to an HTML file
    logger.warning(filename)
    return m

def create_heatmap_for_cloud(tiles_gdf, existing_fig=None):
    if tiles_gdf.empty:
        logger.warning("No items found.")
        return None  # or however you want to handle an empty response

    # Reset index to have 'id' as a column for px.choropleth_mapbox
    tiles_gdf.reset_index(inplace=True)

    # Centroid of the entire scene to center the map
    centroid = tiles_gdf.dissolve().centroid[0]

    
    # Create figure if not provided
    if existing_fig is None:
        fig = px.choropleth_mapbox(
            tiles_gdf,
            geojson=tiles_gdf.geometry.__geo_interface__,
            locations=tiles_gdf.index,
            color="eo:cloud_cover",
            hover_data=["eo:cloud_cover"],
            center={'lat': centroid.y, 'lon': centroid.x},
            zoom=9
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
            geojson=tiles_gdf.geometry.__geo_interface__,
            locations=tiles_gdf.index,
            z=tiles_gdf['eo:cloud_cover']
        )
        fig.add_trace(new_trace)
    
    return fig

def create_folium_basemap(capture_grouped_tiles_gdf):
    if capture_grouped_tiles_gdf.empty:
        logging.warning("No Tiles Found")
        return None
    
    # Create a folium map
    center = capture_grouped_tiles_gdf.geometry.unary_union.centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=8)
    
    for idx, row in capture_grouped_tiles_gdf.iterrows():
        coords = [(y, x) for x, y in zip(*row.geometry.exterior.coords.xy)]
        # Create a tooltip using capture date and outcome_id
        tooltip_text = f"{row['capture_date'].strftime('%Y-%m-%dT%H%M%SZ')}, {row['outcome_id']}"
        tooltip = folium.Tooltip(tooltip_text)
        folium.Polygon(coords, color='blue', weight=1, tooltip=tooltip).add_to(m)
        
        # Adding image overlay
        bounds = [list(row.geometry.bounds[1::-1]), list(row.geometry.bounds[3:1:-1])]
        
        image_url = row["thumbnail_url"]
        raster_layers.ImageOverlay(image_url, bounds=bounds).add_to(m)

    return m