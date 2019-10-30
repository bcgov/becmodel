# Installing `becmodel` on GTS

### 1. Download code

While `becmodel` is a pip installable Python module, it has not been published to `pypi` and/or `conda-forge` - downloading the source code is necessary.
Get the latest from github via a web browser or your preferred git client. For example:

    git clone https://github.com/smnorris/becmodel.git

### 2. Configure `conda` and create an isolated environment

Open a `Python Command Prompt` and create a folder for writing `conda` packages (by default, `conda` writes packages to our user profile, this gets *very* big, corrupting the user profile). For example:

    W:
    cd W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019
    mkdir conda_pkgs
    cd becmodel        # the code folder created in step 1 above
    conda create -p becenv -y
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv
    conda config --env --add pkgs_dirs W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\conda_pkgs
    conda config --env --add channels conda-forge

### 3. Install dependencies, `becmodel` and clean up

Install required packages and `becmodel` itself:

    conda install click gdal numpy pandas fiona rasterio geopandas geojson scikit-image xlrd cligj mercantile -y
    pip install bcdata
    pip install -e .

Reactivate the environment so that the `gdal` and `proj` environment variables are set:

    deactivate
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Installing all this stuff really adds up. Clean up what we can:

    conda clean -a
