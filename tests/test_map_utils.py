# Copyright (c) 2023 Satellogic USA Inc. All Rights Reserved.
#
# This file is part of Spotlite.
#
# This file is subject to the terms and conditions defined in the file 'LICENSE',
# which is part of this source code package.

"""Tests for mapUtils."""

# from typing import Dict, Optional

import unittest
import copy

import folium

from shapely.geometry import box, Polygon
import geopandas as gpd
import pandas as pd

import plotly.graph_objects as go

from mapUtils import (
    estimate_zoom_level,
    create_bounding_box,
    create_bounding_box_choropleth,
    create_map,
    update_map_with_tiles,
)

class TestEstimateZoomLevel(unittest.TestCase):
    """Zoom level tests for estimate_zoom_level."""

    def test_large_extent(self):
        """Test by starting big."""
        self.assertEqual(estimate_zoom_level(0, 0, 20, 20), 6)

    def test_medium_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 6, 6), 7)

    def test_small_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 3, 3), 8)

    def test_very_small_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 1.5, 1.5), 9)

    def test_tiny_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 0.75, 0.75), 10)

    def test_miniscule_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 0.375, 0.375), 11)

    def test_microscopic_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 0.1875, 0.1875), 12)

    def test_nanoscopic_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 0.09375, 0.09375), 13)

    def test_infinitesimal_extent(self):
        """Test by getting smaller."""
        self.assertEqual(estimate_zoom_level(0, 0, 0.05, 0.05), 14)


class TestCreateBoundingBox(unittest.TestCase):
    """Bounding box tests for create_bounding_box."""

    def test_normal_conditions(self):
        """Test under normal conditions."""
        center_lat, center_lon = 40.7128, -74.0060  # Example coordinates (New York City)
        width_km = 5  # Example width in kilometers

        bbox = create_bounding_box(center_lat, center_lon, width_km)

        self.assertIsInstance(bbox, Polygon)
        self.assertAlmostEqual(bbox.area, 0.0014986392800794503)
        self.assertAlmostEqual(list(bbox.exterior.coords), [
            (-73.97641396705156, 40.70013658436215),
            (-73.97641396705156, 40.725463387767846),
            (-74.03558603294844, 40.725463387767846),
            (-74.03558603294844, 40.70013658436215),
            (-73.97641396705156, 40.70013658436215)
        ])

    def test_default_width(self):
        """Test with default width."""
        center_lat, center_lon = 40.7128, -74.0060  # Example coordinates

        bbox = create_bounding_box(center_lat, center_lon)

        self.assertIsInstance(bbox, Polygon)
        self.assertAlmostEqual(bbox.area, 0.0005395101538935243)
        self.assertAlmostEqual(list(bbox.exterior.coords), [
            (-73.98824837980132, 40.70520195396158),
            (-73.98824837980132, 40.72039803600522),
            (-74.02375162019868, 40.72039803600522),
            (-74.02375162019868, 40.70520195396158),
            (-73.98824837980132, 40.70520195396158)
        ])

    def test_invalid_input(self):
        """Test with invalid input."""
        with self.assertRaises(ValueError):
            _ = create_bounding_box('invalid', 'invalid')


class TestCreateBoundingBoxChoropleth(unittest.TestCase):
    """Choropleth for bound box tests for create_bounding_box_choropleth."""

    def test_return_types(self):
        """Test return types are correct."""
        bbox, fig = create_bounding_box_choropleth(
            40.7128, -74.0060)  # Example coordinates
        self.assertIsInstance(bbox, dict)
        self.assertIsInstance(fig, go.Figure)

    def test_bbox_structure(self):
        """Test interior data structure is correct."""
        bbox, _ = create_bounding_box_choropleth(40.7128, -74.0060)
        # Check if bbox has the expected keys and structure
        self.assertIn('type', bbox)
        self.assertIn('coordinates', bbox)
        self.assertEqual(bbox['type'], 'Polygon')

    def test_figure_properties(self):
        """Test figure data structure is reasonable."""
        _, fig = create_bounding_box_choropleth(40.7128, -74.0060)
        # Check for some basic properties of the figure, like data length
        self.assertGreater(len(fig.data), 0)
        # Test for specific properties related to the map configuration
        self.assertEqual(fig.layout.mapbox.style, 'carto-positron')


class TestCreateMap(unittest.TestCase):
    """Base Map from lat long and bounding box test for create_map."""

    def setUp(self):
        """Setup the lat lon and bounding box."""
        # Example coordinates (New York City)
        self.lat, self.lon = 40.7128, -74.0060
        self.bbox = box(-74.1, 40.7, -73.9, 40.8)  # Example bounding box

    def test_return_type(self):
        """Create basic folium map."""
        map_obj = create_map(self.lat, self.lon, self.bbox)
        self.assertIsInstance(map_obj, folium.Map)

    def test_map_properties(self):
        """Inspect map properties."""
        map_obj = create_map(self.lat, self.lon, self.bbox)
        # Check if the map is centered correctly
        self.assertEqual(map_obj.location, [self.lat, self.lon])
        # Further checks can be added for zoom level and other properties

    def test_polygon_in_map(self):
        """Ensure polygon is added to the map."""
        # Check if a Polygon layer is added to the map
        map_obj = create_map(self.lat, self.lon, self.bbox)
        polygon_added = any(isinstance(child, folium.vector_layers.Polygon)
                        for child in map_obj._children.values())
        self.assertTrue(polygon_added)


# TODO - remove
# class TestCreateChoroplethMap(unittest.TestCase):

#     def setUp(self):
#         self.lat, self.lon = 40.7128, -74.0060  # Example coordinates

#         coords = [
#             [self.lon-0.1, self.lat-0.1],
#             [self.lon+0.1, self.lat-0.1],
#             [self.lon+0.1, self.lat + 0.1],
#             [self.lon-0.1, self.lat+0.1],
#             [self.lon-0.1, self.lat-0.1]]
#         polygon_coords = [[{"lat": lat, "lon": lon} for lon, lat in coords]]
#         self.bbox_geojson = {  # Example GeoJSON
#             "type": "Polygon",
#             "coordinates": polygon_coords
#         }

#     def test_figure_creation(self):
#         fig = create_choropleth_map(self.lat, self.lon, self.bbox_geojson)
#         self.assertIsInstance(fig, go.Figure)

#     def test_shape_added(self):
#         fig = create_choropleth_map(self.lat, self.lon, self.bbox_geojson)
#         shapes_added = len(fig.layout.shapes) > 0
#         self.assertTrue(shapes_added)

#     def test_marker_added(self):
#         fig = create_choropleth_map(self.lat, self.lon, self.bbox_geojson)
#         markers_added = any(isinstance(trace, go.Scattermapbox)
#                             for trace in fig.data)
#         self.assertTrue(markers_added)

#     def test_layout_properties(self):
#         fig = create_choropleth_map(self.lat, self.lon, self.bbox_geojson)
#         self.assertEqual(fig.layout.mapbox.zoom, 9)
#         self.assertEqual(fig.layout.mapbox.center, {
#                          "lat": self.lat, "lon": self.lon})


class TestUpdateMapWithTiles(unittest.TestCase):

    def setUp(self):
        self.ref_folium_map = folium.Map(
            location=[40.7128, -74.0060], zoom_start=13)
        self.folium_map = folium.Map(
            location=[40.7128, -74.0060], zoom_start=13)
        self.copied_folium_map = self.folium_map
        self.animation_filename = "example_animation.gif"
        self.aoi_bbox = box(-74.1, 40.7, -73.9, 40.8)
        # Creating a sample GeoDataFrame
        data = {'capture_date': pd.to_datetime('2023-01-01'),
                'satl:outcome_id': 'xxxx',
                'freq': 'S',
                'geometry': [self.aoi_bbox], 'cloud_cover': [10]}
        self.tiles_gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
        self.tiles_gdf = self.tiles_gdf.set_index(
            pd.DatetimeIndex(self.tiles_gdf['capture_date']))

        # TODO - revisit the following behaviour
        # update_map_with_tiles changes the passed in map as well as
        # returning the object, too.

    def test_map_update_with_nonempty_gdf(self):
        updated_map = update_map_with_tiles(
            self.folium_map, self.tiles_gdf, self.animation_filename, self.aoi_bbox)

        self.assertIsInstance(updated_map, folium.Map)
        self.assertEqual(len(updated_map._children) - len(
            self.ref_folium_map._children), 2)  # Check if new layers are added

    # def test_map_update_with_empty_gdf(self):
    #     empty_gdf = gpd.GeoDataFrame(crs="EPSG:4326")
    #     updated_map = update_map_with_tiles(
    #         self.folium_map, empty_gdf, self.animation_filename, self.aoi_bbox)
    #     self.assertIsNone(updated_map)

    def test_polygon_addition_for_polygon_geometry(self):
        # Assuming the sample GeoDataFrame has polygon geometries
        updated_map = update_map_with_tiles(
            self.folium_map, self.tiles_gdf, self.animation_filename, self.aoi_bbox)
        polygon_added = any(isinstance(child, folium.vector_layers.Polygon)
                            for child in updated_map._children.values())
        self.assertTrue(polygon_added)


if __name__ == '__main__':
    unittest.main()
