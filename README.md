# becmodel

## Background

The Large Scale Biogeoclimatic Ecosystem Classification Process generates biogeoclimatic ecosystem classifications for subzones/variants at a TRIM 1:20,000 scale.


## Installation

Installation is easiest with conda, see the [guide](doc/conda_guide.md). (Note that you can use the built in conda [Python Package Manager](https://pro.arcgis.com/en/pro-app/arcpy/get-started/what-is-conda.htm) if you have ArcGIS Pro installed.

Alternatively, install via `pip install` if:

- Python and pip are already installed
- you are comfortable with managing your Python environment
- if using Windows, you have [manually downloaded and installed the correct pre-compiled gdal/fiona/rasterio wheels](https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal)

## Required files

### Rule polygon file

A polygon layer where each polygon represents a unique combination of elevations rules for the occuring BGC units. The file must meet the following requirements:

- *format*: Any format readable by `fiona` (FileGDB, GPKG, SHP)
- *projection*: BC Albers (`EPSG:3005`)
- *required attribute*: `GRIDCODE`

### Elevation file

A table (csv format) with the following columns:


### BEC master file

A table (csv format) with the following columns:


### Config file

A text file that defines the parameters for the model run, overriding the defaults. A sample file is here [`sample_config.cfg`](sample_config.cfg)



## Usage

Modify your config file as required and provide path of the config file as an argument to the script:

      $ becmodel --help
      Usage: becmodel [OPTIONS] CONFIG_FILE

      Options:
        -v, --validate
        --help          Show this message and exit.
      (becmodel-env)

      $ becmodel sample_config.cfg
      2019-03-06 11:58:41,881 becmodel.cli INFO     Loading config from file: sample_config.cfg
      2019-03-06 11:58:41,883 becmodel.cli INFO     Running the bec model

Temp data are written to the default workspace `becmodel_tempdata` or to the folder specified by the `wskp` key in the config file.