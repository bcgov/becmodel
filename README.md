# becmodel

## Background

The Large Scale Biogeoclimatic Ecosystem Classification Process generates biogeoclimatic ecosystem classifications for subzones/variants at a TRIM 1:20,000 scale.


## Installation

Installation is easiest with Anaconda, see the [guide](doc/conda_guide.md).

Alternatively, installing via `pip` is possible if:
- Python and pip are already installed
- you are comfortable with managing your environment
- you are not using Windows

## Required files

### Rule polygon file

A polygon layer where each polygon represents a unique combination of elevations rules for the occuring BGC units. The file must meet the following requirements:

- *format*: Any format readable by `fiona` (FileGDB, GPKG, SHP)
- *projection*: BC Albers (`EPSG:3005`)
- *required attribute*: `grid_code`

### Elevation file

A table (csv format) with the following columns:


### BEC master file

A table (csv format) with the following columns:


### Config file

A text file that defines the parameters for the model run.