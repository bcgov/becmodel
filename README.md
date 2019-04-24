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
- include these attributes of noted types:

        polygon_number      : integer
        polygon_description : string / character

### Elevation file

A table (csv format) with the following columns (in any order, case insensitive):


    becvalue       : integer
    beclabel       : string
    class_name     : string
    cool_low       : integer
    cool_high      : integer
    neutral_low    : integer
    neutral_high   : integer
    warm_low       : integer
    warm_high      : integer
    polygon_number : integer



### BEC master file

A table (csv format) with the following columns (in any order, case insensitive):

    becvalue : integer
    beclabel : string
    zone     : string
    subzone  : string
    variant  : string
    phase    : string


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
      2019-04-19 22:11:54,115 becmodel.cli INFO     Initializing BEC model v0.0.3dev
      2019-04-19 22:11:54,115 becmodel.cli INFO     Loading config from file: sample_config.cfg
      2019-04-19 22:11:54,975 becmodel.main INFO     becmodel_tempdata/becvalue.shp created

Temp data are written to the default workspace `becmodel_tempdata` or to the folder specified by the `wskp` key in the config file.