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


## Usage

Before running the script, prepare the required files listed below:

### 1. Rule polygons

A polygon layer where each polygon represents a unique combination of elevations rules for the occuring BGC units. The file must:

- be a format readable by [`fiona`](https://github.com/Toblerity/Fiona) (`ESRI FileGDB`, `Geopackage`, `ESRI Shapefile`, `GeoJSON`, etc)
- include a polygon number attribute (both long name and short name are accepted):

        polygon_number | polygonnbr  : integer

All internal files and outputs are in the BC Albers (`EPSG:3005`) coordinate system, the tool will attempt to reproject the input rule polygons to BC Albers if the provided layer uses some other coordinate system.

See [example rule polygon layer](examples/robson/rulepolys.geojson)

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

A text [initialization file](https://docs.python.org/3/library/configparser.html#supported-ini-file-structure) that defines the parameters for the model run, overriding the defaults. The file must include the `[CONFIG]` section header. The file may have any file name or extension - file extensions `ini`, `cfg`, `txt` are all valid.

See example config files:

- [Example 1 - all available parameters](sample_config.cfg)
- [Example 2 - project specific](examples/robson/robson.cfg)


### 4. Optional - `bec_biogeoclimatic_catalogue.csv`

If your project contains beclabels not already in the warehouse, define the new beclabels in this file. The file is available to download via the [DataBC Catalogue](https://catalogue.data.gov.bc.ca/dataset/bec-map-attribute-catalogue) or [from this code repository](becmodel/data/bec_biogeoclimatic_catalogue.csv). Modify the table as required if creating new beclabels. To use this file, you must define the `becmaster` key in your config file as the full path to this file on the system. For example:

`becmaster = C:\projects\bec\custom_bec_master.csv`

This file must:

- be in .csv format
- include these columns:
    + `biogeoclimatic_catalogue_id`
    + `zone`
    + `subzone`
    + `variant`
    + `phase`
- beclabel (combined `zone`/`subzone`/`variant`/`phase`) values must contain all beclabels present in your project
- beclabel (combined `zone`/`subzone`/`variant`/`phase`) values must be unique
- `biogeoclimatic_catalogue_id` values must be unique


###  Running the model

On GTS, open a `Python Command Prompt` window and activate the `becenv` conda environment:

    (arcgispro-py3)> activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Consider navigating to your project folder, eg:

        (becenv)> W:
        (becenv)> cd W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel

Finally, run the `becmodel` script with the path to your config file as an argument to the script:


    (becenv)> becmodel tests/test.cfg
    becmodel.main INFO     Downloading and processing DEM
    becmodel.main INFO     Generating initial becvalue raster
    becmodel.main INFO     Running majority filter
    becmodel.main INFO     Running noise removal filter
    becmodel.main INFO     Running morphology.area_closing() to clean results of noise filter
    becmodel.main INFO     Running high_elevation_removal_threshold on alpine
    becmodel.main INFO     Running high_elevation_removal_threshold on parkland
    becmodel.main INFO     Running high_elevation_removal_threshold on woodland
    becmodel.main INFO     Running majority filter again to tidy edges
    becmodel.main INFO     Running noise filter again to clean results of majority filter
    becmodel.main INFO     Output bectest.gpkg created

Temporary files (`dem.tif`, `aspect.tif` etc) are written to the folder `tempdata`,
or as specified by the `temp_folder` key in the config file.

The script includes several options:

    (becenv)>becmodel --help
    Usage: becmodel [OPTIONS] [CONFIG_FILE]

    Options:
      -dr, --dry_run, --dry-run  Validate inputs - do not run model
      -l, --load                 Download input datasets - do not run model
      -o, --overwrite            Overwrite existing outputs
      -qa, --qa                  Write temp files to disk for QA
      -v, --verbose              Increase verbosity.
      -q, --quiet                Decrease verbosity.
      --help                     Show this message and exit.