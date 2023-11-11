# spotlite - Satellogic Imagery Discovery and Access Demonstration Tool

## PURPOSE:
This app is intended to exercise the API in a demo centric way
that allows the user to follow their predicted user CONOPS lifecycle (below).

Life Cycle Steps (based on the [TCPED](https://www.dhs.gov/sites/default/files/publications/FactSheet%20TCPED%20Process%20Analysis%202016.12.12%20FINAL_1_0.pdf) life cycle):
1. User enters place name or lat/long for search
2. Search the archive for tiles and visualize them.
3. Access the full resolution Rapid Response products
4. Animate tiles time sequence for context and change monitoring
5. Order different product formats
6. Create new subscription areas to monitor for images coming in
7. Analyze tiles to extract analytics/information/intelligence
8. Create new tasking activities for high priority POIs
9. Repeat

## MAIN FUNCTION ACTIVITIES

The main functions provided in the spotlite_main.py are captured below.

The menus are:

Options:
1. Search And Animate Site
2. Search And Plot Images With Thumbnails.
3. Create Cloud Free Basemap.
4. Create Heat Map Of AOI For Collection Age.
5. Create Heatmap Of Imagery Depth.
6. Download Tiles For BBox
7. Manage Subscriptions
8. Enter New Tasking
9. q for Quit...

## INSTALLATION

The user of this app needs to run the python script within a setup environment.
You will need to setup the environment in a virtual environment:

### Linux Installation

#### Install GDAL with brew

```bash
brew update
brew upgrade
brew install gdal
brew doctor
```

### Setup virtualenv

```bash
python -m venv venv
. ./venv/bin/activate
pip install -r requirements.txt
```

### Windows installation

Conda is required.  Ensure it is installed on your machine, then perform the following.

```bash
conda env create -f envionment.yaml
```

## HOW TO RUN APPLICATION

Then you should be able to run the app.

```bash
python ./spotlite_main.py
```

You follow the prompts from there.  Some functions are more mature than others.
Search and Animate Site, Create Heatmaps, Download Tiles are my favorite.
For example Create Cloud Free Basemap seems to be a good idea but no workable in practice.

Other services in this app that need to be started and left running in your terminal

```bash
python ./indicationsAndWarningsSrvc.py
```

Searches ~500 POI that were marked and validated for new imagery that comes in during the last period.  It runs a search, creates animation of time series, before and after image

```bash
python ./monitoringSrvc.py
```

This is managed in the main menu where you can create new monitoring areas to listen for imagery

For example "I want to be notified when new imagery arrives over Gaza".  You need to do more work to set this up though!  It sends an email using gmail, but you will need to use the Google Developer Console to set that up to get the credentials file.

```bash
python ./imageDepthAgeSrvc.py
```

This runs periodically to rebuild plots for imagery depth and count.
## config.py file contents

Place at root dir.

```bash
# Define your configurations here, for example:
INTERNAL_STAC_API_URL = "GetFromSatellogic"
STAC_API_URL = "https://api.satellogic.com/archive/stac"
SUBSCRIPTIONS_FILE_PATH = 'points_to_monitor/subscriptions.geojson'
SUBC_MON_FREQUENCY = 120 # Minutes between subscription monitor runs.
MAP_UPDATE_FREQUENCY = 360 # Minutes between updates aka 24 hours.
CLOUD_THRESHOLD = 30 # Will reject captures with 20% or more CC when animating.
PERIOD_BETWEEN_FRAMES = 3 #seconds between animation frames
EMAIL_PASSWORD = ""
EMAIL_ADDRESS = ""
KEY_ID = "GetFromSatellogic"
KEY_SECRET = "GetFromSatellogic"
MIN_PRODUCT_VERSION = "1.0.0" # Min required product version.
MIN_TILE_COVERAGE_PERCENT = 0.01 # Min percent of tile coverage 0.6 = 60% - FYI if you use 1 width a lot of images will be skipped.
VALID_PIXEL_PERCENT_FOR_BASEMAP = 100 # This is used for making basemaps, checks that tiles have full coverage otherwise drops them from the basemap.
MONITORING_POINTS_FILE_PATH = "points_to_monitor/points_file.geojson" # A geojson file containing files to include in systematic monitoring.
POLYGON_FILES_PATH = "points_to_monitor/age_count_polygons_file.geojson" # A geojson file containing polygons of AOIs to update age count graphics.
DEFAULT_BOUNDING_BOX_WIDTH = 3 # Default width of bounding box in km.
DAYS_TO_SEACH_FOR_BEFORE_IMAGERY = 60 # Days to search for historical imagery for the Indications and Warnings app.
IS_INTERNAL_TO_SATL = False # If True then use the behind the filewall connection -SATL only-, otherwise use credentials.
```
