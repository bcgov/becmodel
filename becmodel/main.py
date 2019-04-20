import os
import math
import logging
import shutil
import subprocess
from math import ceil

import fiona
import rasterio
from rasterio import features
from rasterio.transform import Affine
from rasterio.warp import Resampling, reproject
from rasterio.features import shapes
from osgeo import gdal
import numpy as np
from skimage.filters.rank import majority
from skimage.morphology import disk

import bcdata
from becmodel import util
from becmodel.config import config


log = logging.getLogger(__name__)


def process(overwrite=False):
    """ Generate becvalue raster from rules and DEM
    """

    # get bounds and expand by specified distance
    with fiona.open(config["rulepolys_file"], layer=config["rulepolys_layer"]) as src:
        bump = config["expand_bounds"]
        bounds = [
            math.trunc(b)
            for b in [
                src.bounds[0] - bump,
                src.bounds[1] - bump,
                src.bounds[2] + bump,
                src.bounds[3] + bump,
            ]
        ]

    if overwrite and os.path.exists(config["wksp"]):
        shutil.rmtree(config["wksp"])

    util.make_sure_path_exists(config["wksp"])

    # define paths to output files - just to make code a bit more compact
    dem = os.path.join(config["wksp"], "dem.tif")
    slope = os.path.join(config["wksp"], "slope.tif")
    aspect = os.path.join(config["wksp"], "aspect.tif")
    aspect_class = os.path.join(config["wksp"], "aspect_class.tif")
    rules = os.path.join(config["wksp"], "rules.tif")
    becvalue = os.path.join(config["wksp"], "becvalue.shp")

    # remove all if overwrite specified
    if overwrite:
        for tif in [dem, slope, aspect, aspect_class, rules, becvalue]:
            if os.path.exists(tif):
                os.remove(tif)

    # get dem, generate slope and aspect
    if not os.path.exists(dem):
        bcdata.get_dem(bounds, dem)
    if not os.path.exists(aspect):
        gdal.DEMProcessing(aspect, dem, "aspect", zeroForFlat=True)
    if not os.path.exists(slope):
        gdal.DEMProcessing(slope, dem, "slope", slopeFormat="percent")

    # load slope
    with rasterio.open(slope) as src:
        slope_image = src.read(1)

    # classify aspect
    with rasterio.open(aspect) as src:
        array1 = src.read(1)
        # set aspect to -1 for all slopes less that 15%
        array1[slope_image < 15] = -1
        aspect_image = array1.copy()
        profile = src.profile
        for aspect in config["aspects"]:
            for rng in aspect["ranges"]:
                aspect_image[
                    (array1 >= rng["min"]) & (array1 < rng["max"])
                ] = aspect["code"]

    if not os.path.exists(aspect_class):
        with rasterio.open(aspect_class, "w", **profile) as dst:
            dst.write(aspect_image, 1)

    # load dem into memory and get the shape and affine transform
    # (so new rasters line up)
    with rasterio.open(dem) as src:
        shape = src.shape
        l, b, r, t = src.bounds
        transform = src.transform
        height = src.height
        width = src.width
        crs = src.crs
        dem_image = src.read(1)

    # burn rule polygon number to raster using above DEM shape/transform
    with fiona.open(config["rulepolys_file"], layer=config["rulepolys_layer"]) as src:

        rules_image = features.rasterize(
            ((s["geometry"], int(s["properties"]["polygon_number"])) for s in src),
            out_shape=shape,
            transform=transform,
            all_touched=False,
        )
        with rasterio.open(
            rules,
            "w",
            driver="GTiff",
            dtype=rasterio.uint16,
            count=1,
            width=width,
            height=height,
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(rules_image, indexes=1)

    # generate becvalue raster by iterating through elevation table,
    # setting output raster to becvalue for each row where criteria are met
    # by the dem/aspect/rulepolys
    becvalue_image = np.zeros(shape=shape, dtype="uint16")
    data = util.load_tables()
    for index, row in data["elevation"].iterrows():
        for aspect in config["aspects"]:
            becvalue_image[
                (rules_image == row["polygon_number"]) &
                (aspect_image == aspect["code"]) &
                (dem_image >= row[aspect["name"]+"_low"]) &
                (dem_image < row[aspect["name"]+"_high"])
            ] = row["becvalue"]

    # Smooth by applying majority filter to output
    # Note that skimage.filters.rank.majority is unreleased, it was merged
    # to skimage master just 4 days ago (April 19)
    becvalue_image = np.where(
        slope_image < 25,
        majority(becvalue_image, disk(5)),
        majority(becvalue_image, disk(3))
    )

    # Resample data to specified cell size
    # https://github.com/mapbox/rasterio/blob/master/rasterio/rio/warp.py
    res = (config["cell_size"], config["cell_size"])
    dst_transform = Affine(res[0], 0, l, 0, -res[1], t)
    dst_width = max(int(ceil((r - l) / res[0])), 1)
    dst_height = max(int(ceil((t - b) / res[1])), 1)
    becvalue_resampled = np.empty(
        shape=(dst_height, dst_width),
        dtype='uint16'
    )
    reproject(
        becvalue_image,
        becvalue_resampled,
        src_transform=transform,
        dst_transform=dst_transform,
        src_crs=crs,
        dst_crs=crs,
        resampling=Resampling.nearest
    )

    # don't write resulting image for now, just needed for testing
    #with rasterio.open(
    #        becvalue,
    #        "w",
    #        driver="GTiff",
    #        dtype=rasterio.uint16,
    #        count=1,
    #        width=dst_width,
    #        height=dst_height,
    #        crs=crs,
    #        transform=dst_transform,
    #) as dst:
    #    dst.write(becvalue_resampled, indexes=1)

    # write to shapefile
    results = (
            {'properties': {'becvalue': v}, 'geometry': s}
            for i, (s, v)
            in enumerate(
                shapes(becvalue_resampled, transform=dst_transform))
    )
    with fiona.open(
            becvalue, 'w',
            driver="ESRI Shapefile",
            crs=crs,
            schema={'properties': [('becvalue', 'int')],
                    'geometry': 'Polygon'}) as dst:
        dst.writerecords(results)
