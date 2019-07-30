# becmodel

## Background

The Large Scale Biogeoclimatic Ecosystem Classification Process generates biogeoclimatic ecosystem classifications for subzones/variants at a TRIM 1:20,000 scale.


## Installation

Installation is easiest with conda, see the [guide](doc/conda_guide.md). (Note that you can use the built in conda [Python Package Manager](https://pro.arcgis.com/en/pro-app/arcpy/get-started/what-is-conda.htm) if you have ArcGIS Pro installed and are not on a shared system.)

Alternatively, install via `pip install` if:

- Python and `pip` are already installed
- you are comfortable with managing your Python environment
- if using Windows, you have [manually downloaded and installed the correct pre-compiled gdal/fiona/rasterio wheels](https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal)
- a C++ compiler is available (required to install `scikit-image` from master branch, we need unreleased features)
- install in this order so that `scikit-image` finds `numpy` and `cython`:

        pip install numpy
        pip install cython
        pip install -r requirements.txt


## Required files

### Rule polygon file

A polygon layer where each polygon represents a unique combination of elevations rules for the occuring BGC units. The file must:

- be a format readable by [`fiona`](https://github.com/Toblerity/Fiona) (`ESRI FileGDB`, `Geopackage`, `ESRI Shapefile`, `GeoJSON`, etc)
- include a polygon number attribute (both long name and short name are accepted):

        polygon_number | polygonnbr  : integer

All internal files and outputs are in the BC Albers (`EPSG:3005`) coordinate system, the tool will attempt to reproject the input rule polygons to BC Albers if the provided layer uses some other coordinate system.

See [example rule polygon layer](examples/robson/rulepolys.geojson)

### Elevation file

A table (one of csv/xls/xls formats) with the following columns (in any order, case insensitive, short names also accepted where noted)


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

### Config file

A text file that defines the parameters for the model run, overriding the defaults.

See example config files:

- [example 1](sample_config.cfg)
- [example 2](examples/robson/robson.cfg)



## Usage

Modify your config file as required and provide path of the config file as an argument to the script:

      $ becmodel --help
        Usage: becmodel [OPTIONS] CONFIG_FILE

        Options:
          -v, --validate
          -o, --overwrite
          --help           Show this message and exit.

      $ becmodel sample_config.cfg
        becmodel.main INFO     Downloading and processing DEM
        becmodel.main INFO     Generating initial becvalue raster
        becmodel.main INFO     Running majority filter
        becmodel.main INFO     Running noise removal filter
        becmodel.main INFO     Running high elevation minimum size filter
        becmodel.main INFO     Output becmodel_tempdata/becmodel.gpkg created

Temp data are written to the default workspace `becmodel_tempdata` or to the folder specified by the `temp_folder` key in the config file.