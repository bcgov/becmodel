import os
import math
import logging


import fiona
import rasterio
from rasterio import features
from osgeo import gdal
import numpy as np

import bcdata
from becmodel import util
from becmodel.config import config


log = logging.getLogger(__name__)


def load():
    """ rasterize rules polygon layer, get DEM, calc aspect
    """

    # get bounds and expand by specified distance
    with fiona.open(config["rulepolygon_file"], layer=config["rulepolyon_layer"]) as src:
        bump = config["expand_bounds"]
        bounds = [math.trunc(b) for b in [src.bounds[0] - bump, src.bounds[1] - bump, src.bounds[2] + bump, src.bounds[3] + bump]]

    util.make_sure_path_exists(config["wksp"])

    # define paths to output paths to make code more compact
    dem = os.path.join(config["wksp"], "dem.tif")
    aspect = os.path.join(config["wksp"], "aspect.tif")
    aspect_class = os.path.join(config["wksp"], "aspect_class.tif")
    rules = os.path.join(config["wksp"], "rules.tif")

    # get dem, calculate aspect
    if not os.path.exists(dem):
        bcdata.get_dem(bounds, dem)
    if not os.path.exists(aspect):
        gdal.DEMProcessing(aspect, dem, 'aspect')

    # classify aspect
    # https://gis.stackexchange.com/questions/163007/raster-reclassify-using-python-gdal-and-numpy
    if not os.path.exists(aspect_class):
        with rasterio.open(aspect) as src:
            array1 = src.read()
            array2 = array1.copy()
            profile = src.profile
            array2[np.where(np.logical_and(array1 >= 0, array1 < 45))] = 100
            array2[np.where(np.logical_and(array1 >= 45, array1 < 135))] = 200
            array2[np.where(np.logical_and(array1 >= 135, array1 < 270))] = 300
            array2[np.where(np.logical_and(array1 >= 270, array1 < 315))] = 200
            array2[np.where(np.logical_and(array1 >= 315, array1 < 361))] = 100

        with rasterio.open(aspect_class, 'w', **profile) as dst:
            dst.write(array2)

    # get the shape and affine transform of the DEM so new rasters line up
    with rasterio.open(dem) as src:
        shape = src.shape
        transform = src.transform
        height = src.height
        width = src.width
        crs = src.crs

    # burn rule polygon GRIDCODE to raster using above DEM shape/transform
    with fiona.open(config["rulepolygon_file"], layer=config["rulepolyon_layer"]) as src:

        image = features.rasterize(
            ((s['geometry'], int(s['properties']['age'])) for s in src),
            out_shape=shape,
            transform=transform,
            all_touched=False
        )
        with rasterio.open(
            rules, 'w',
            driver='GTiff',
            dtype=rasterio.uint16,
            count=1,
            width=width,
            height=height,
            crs=crs,
            transform=transform
        ) as dst:
            dst.write(image, indexes=1)
