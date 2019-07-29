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

- be a format readable by [`fiona`](https://github.com/Toblerity/Fiona) (`FileGDB`, `GPKG`, `SHP`, etc)
- use the BC Albers (`EPSG:3005`) coordinate reference system / projection
- include this attribute:

        polygon_number      : integer

### Elevation file

A table (one of csv/xls/xls formats) with the following columns (in any order, case insensitive):


    beclabel       : string
    cool_low       : integer
    cool_high      : integer
    neutral_low    : integer
    neutral_high   : integer
    warm_low       : integer
    warm_high      : integer
    polygon_number : integer

If using an Excel file, the elevation table data must be in the first worksheet, with data originating at Column A, Row 1.

### Config file

A text file that defines the parameters for the model run, overriding the defaults.

See sample file [`sample_config.cfg`](sample_config.cfg)



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