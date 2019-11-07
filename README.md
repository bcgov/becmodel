# becmodel

## Background

The Large Scale Biogeoclimatic Ecosystem Classification Process generates biogeoclimatic ecosystem classifications for subzones/variants at a TRIM 1:20,000 scale.


## Installation


### Install with `conda`

Installation is generally easiest with conda.  See [this guide](doc/gts_install.md) for how to install on a GTS Windows server.

### Install with `pip`

Alternatively, install with `pip` if you are comfortable with managing your Python environment yourself. Installation to a virtual environment will be something like this:

        virtualenv becmodelvenv
        source becmodelven/bin/activate
        git clone https://github.com/smnorris/becmodel.git
        cd becmodel
        pip install .

Note that if you are installing via pip on Windows, you will need to manually download and install the correct pre-compiled wheels for [`gdal`, `fiona` and `rasterio`](https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal) before installing `becmodel`.


## Data Prep

Before running the script, prepare the required files listed below:

### 1. Rule polygons

A polygon layer where each polygon represents a unique combination of elevations rules for the occuring BGC units. The file must:

- be a format readable by [`fiona`](https://github.com/Toblerity/Fiona) (`ESRI FileGDB`, `Geopackage`, `ESRI Shapefile`, `GeoJSON`, etc)
- include a polygon number attribute (both long name and short name are accepted):

        polygon_number | polygonnbr  : integer

All internal files and outputs are in the BC Albers (`EPSG:3005`) coordinate system, the tool will attempt to reproject the input rule polygons to BC Albers if the provided layer uses some other coordinate system.

See [example rule polygon layer](tests/data/rulepolys_4326.geojson)

### 2. Elevation table

A table (one of csv/xls/xlsx formats) with the following columns (in any order, case insensitive, short names also accepted where noted)


    beclabel                     : string
    cool_low                     : integer
    cool_high                    : integer
    neutral_low    | neut_low    : integer
    neutral_high   | neut_high   : integer
    warm_low                     : integer
    warm_high                    : integer
    polygon_number | polygonnbr  : integer

If using an Excel file, the elevation table data must be in the first worksheet, with header originating at Column A, Row 1 and data originating at Column A, Row 2.

See [example elevation file](examples/robson/elevation.csv)

### 3. Configuration / initialization file

A text [initialization file](https://docs.python.org/3/library/configparser.html#supported-ini-file-structure) that defines the parameters for the model run, overriding the defaults. The file must include the `[CONFIG]` section header. The file may have any file name or extension - file extensions `ini`, `cfg`, `txt` are all valid. Note that the config file does not have to contain all parameters, you only need to include those where you do not wish to use the [default values](becmodel/config.py).

See [example config file](sample_config.cfg) listing all configuration parameters.


### 4. `bec_biogeoclimatic_catalogue.csv` (optional)

If your project contains BEC labels/values not already in the provincial table in the BCGW (`WHSE_FOREST_VEGETATION.BEC_BIOGEOCLIMATIC_CATALOGUE`), define the new labels/values in this file. The source file is available to download via the [DataBC Catalogue](https://catalogue.data.gov.bc.ca/dataset/bec-map-attribute-catalogue) and a static version is included in [becmodel/data/bec_biogeoclimatic_catalogue.csv](becmodel/data/bec_biogeoclimatic_catalogue.csv). Modify the table as required if creating new labels/values. To use this file, you must define the `becmaster` key in your config file as the full path to this file on the system. For example, the config would look something like this:

`becmaster = C:\projects\bec\custom_bec_master.csv`

This file must:

- be in .csv format
- include these columns:
    + `biogeoclimatic_catalogue_id`
    + `zone`
    + `subzone`
    + `variant`
    + `phase`
- have beclabel values (combined `zone`/`subzone`/`variant`/`phase`) that are unique and contain all beclabels present in your elevation table

### 5. DEM (optional)

By default, the model downloads TRIM DEM data as geotiff from the `bc_elevation_25m_bcalb` in the [BC Web Coverage Service](https://delivery.openmaps.gov.bc.ca/om/wcs) (WCS) as needed. When necessary, it will also default to downloading DEM data outside of BC from the Mapzen [`terrain-tiles`](http://s3.amazonaws.com/elevation-tiles-prod/geotiff).

To use a custom DEM, add the `dem` key to your configuration file, with the full path to your custom DEM file as the value. Any raster file format supported by GDAL should work.

**Custom DEM notes**

`becmodel` does not check to see if the profile of the provided DEM matches the related keys in your config. Because all `becmodel` processing is based on the DEM, you should manually:

- ensure your DEM covers your area of interest plus config `expand_bounds_metres`
- resample your DEM to a resolution that matches config `cell_size_metres`
- reproject your DEM to BC Albers (`EPSG:3005`)
- align your DEM to the HectaresBC grid default

For example, see [`scripts/create_test_dem.sh`](scripts/create_test_dem).

Also note that if a DEM file is provided to `becmodel` config, `becmodel` will not attempt to download non-BC data from Mapzen terrain-tiles. If you require elevation data outside of BC, integrate it into your DEM before running the model.


##  Running the model

On GTS, open a `Python Command Prompt` window and activate the `becenv` conda environment:

    (arcgispro-py3)> activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Consider navigating to your project folder, eg:

        (becenv)> W:
        (becenv)> cd W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\sample_projects\robson

Finally, run the `becmodel` script with the path to your config file as an argument to the script:


    (becenv)> becmodel robson_mini.cfg
    2019-10-30 12:27:27,399 becmodel.main INFO     Initializing BEC model v0.1.0dev0
    2019-10-30 12:27:27,399 becmodel.main INFO     Loading config from file: robson_mini.cfg
    2019-10-30 12:27:27,480 becmodel.util INFO     Input data is not specified as BC Albers, attempting to reproject
    2019-10-30 12:27:27,659 becmodel.main INFO     Temp data are here: tempdata
    2019-10-30 12:27:27,661 becmodel.main INFO     Downloading and processing DEM
    2019-10-30 12:27:27,661 becmodel.main INFO     Bounds: 1341987.5 916287.5 1374787.5 938787.5
    2019-10-30 12:27:32,280 becmodel.main INFO     Generating initial becvalue raster:
    2019-10-30 12:27:32,655 becmodel.main INFO     Running majority filter
    2019-10-30 12:27:32,812 becmodel.main INFO     Running noise removal filter
    2019-10-30 12:27:32,976 becmodel.main INFO     Running high_elevation_removal_threshold on alpine
    2019-10-30 12:27:33,021 becmodel.main INFO     Running high_elevation_removal_threshold on parkland
    2019-10-30 12:27:33,065 becmodel.main INFO     Running high_elevation_removal_threshold on woodland
    2019-10-30 12:27:33,606 becmodel.main INFO     QA files are here: tempdata
    2019-10-30 12:27:33,687 becmodel.main INFO     Logging config to here: becmodel-config-log_2019-10-30T12-27-27.txt
    2019-10-30 12:27:33,687 becmodel.main INFO     Output robson_mini.gpkg created

Temporary files are written to the folder specified by the `temp_folder` key in the config file. The script writes all configuration options used for the model run to a text file named: `becmodel-config-log_<DATE>T<TIME>.txt`

The script includes several options, `becmodel --help` lists them all:

    (becenv)> becmodel --help
    Usage: becmodel [OPTIONS] [CONFIG_FILE]

    Options:
      -dr, --dry_run, --dry-run       Validate inputs - do not run model
      -l, --load                      Download input datasets - do not run model
      -o, --overwrite                 Overwrite any existing DEM, aspect, slope
                                      files
      -d, --discard-temp, --discard_temp
                                      Do not write temp files to disk
      -v, --verbose                   Increase verbosity.
      -q, --quiet                     Decrease verbosity.
      --help                          Show this message and exit.