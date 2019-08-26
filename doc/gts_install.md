# Build the conda environment on GTS

Get latest becmodel code:

    git clone https://github.com/smnorris/becmodel.git
    cd becmodel

Create conda environment:

    conda create -p becenv -y
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Add conda-forge channel and make sure we don't write packages to user profile (this gets *very* big, corrupting the user profile)

    conda config --env --add channels conda-forge
    mkdir becenv\pkgs
    conda config --env --add pkgs_dirs W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv\pkgs

Install required packages and becmodel itself:

    conda install click numpy gdal rasterio fiona pandas geopandas geojson xlrd scipy -y
    pip install https://www.hillcrestgeo.ca/outgoing/public/scikit_image-0.16.dev0-cp37-cp37m-win_amd64.whl
    pip install -e .

Reactivate the environment so that the gdal and proj environment variables are set:

    deactivate
    activate W:\FOR\VIC\HRE\Projects\Landscape\ProvBGC\CurrentWork\TestingNewBECmodel2019\becmodel\becenv

Installing all this stuff really adds up. Clean up what we can:

    conda clean -a
