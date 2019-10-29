# Build the conda environment on GTS

Get latest becmodel code from github using a web browser, or from a `bash` command line:

    git clone https://github.com/smnorris/becmodel.git

Open a `Python Command Prompt` and create a folder for writing conda packages - we don't want to write conda packages to our user profile, this gets *very* big, corrupting the user profile. For example:

    W:
    cd W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019
    mkdir conda_pkgs
    cd becmodel
    conda create -p becenv -y
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv
    conda config --env --add pkgs_dirs W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\conda_pkgs
    conda config --env --add channels conda-forge

Install required packages and becmodel itself:

    conda install click gdal numpy pandas fiona rasterio geopandas geojson scikit-image xlrd cligj mercantile -y
    pip install bcdata
    pip install -e .

Reactivate the environment so that the gdal and proj environment variables are set:

    deactivate
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Installing all this stuff really adds up. Clean up what we can:

    conda clean -a
