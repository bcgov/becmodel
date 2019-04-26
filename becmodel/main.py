import os
import math
import logging
import shutil
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
from skimage.morphology import disk, remove_small_objects
from skimage.measure import label

import bcdata
from becmodel import util
from becmodel.config import config


log = logging.getLogger(__name__)


def process(overwrite=False, qa=False):
    """ Generate becvalue file from rules and DEM
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
    becvalue = os.path.join(config["wksp"], "becvalue.gpkg")

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
        array1[slope_image < config["flat_aspect_slope_threshold"]] = -1
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

    # load and validate inputs (rule polys, elevation table, becmaster)
    data = util.load_tables()

    # burn rule polygon number to raster using above DEM shape/transform
    rules_image = features.rasterize(
        ((geom, value) for geom, value in zip(data["rulepolys"].geometry, data["rulepolys"].polygon_number)),
        out_shape=shape,
        transform=transform,
        all_touched=False,
        dtype=np.uint16,
    )

    # generate becvalue raster by iterating through elevation table,
    # setting output raster to becvalue for each row where criteria are met
    # by the dem/aspect/rulepolys
    becvalue_image = np.zeros(shape=shape, dtype="uint16")
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
    becvalue_filtered = np.where(
        slope_image < config["majority_filter_steep_slope_threshold"],
        majority(becvalue_image,
                 disk(config["majority_filter_low_slope_radius"])),
        majority(becvalue_image,
                 disk(config["majority_filter_steep_slope_radius"]))
    )

    # Remove areas smaller than noise removal threshold
    # first, convert noise_removal_threshold value from ha to cells
    # (based on 25m dem = 625m2 cell)
    noise_threshold = int(
        config["noise_removal_threshold"] / 625
    )

    # now find unique cell groupings (like converting to singlepart)
    becvalue_labels = label(becvalue_filtered, connectivity=1)

    # identify the areas smaller than noise removal threshold
    mask = remove_small_objects(becvalue_labels, noise_threshold)

    # Fill in the masked areas by again applying a majority filter.
    # But, this time,
    # - exclude areas smaller than noise threshold from majority filter calc
    # - use 10 cell radius filter
    # - only use the result to fill in the holes
    becvalue_cleaned = np.where(
        (mask == 0) & (becvalue_filtered > 0),
        majority(becvalue_filtered, disk(10), mask=mask),
        becvalue_filtered
    )

    # if specified, dump intermediate rasters to disk for review
    if qa:
        for raster in [
            "becvalue_image",
            "becvalue_filtered",
            "becvalue_labels",
            "mask",
            "becvalue_cleaned",
            "rules_image"
        ]:
            with rasterio.open(
                os.path.join(config["wksp"], raster+".tif"),
                "w",
                driver="GTiff",
                dtype=rasterio.uint16,
                count=1,
                width=width,
                height=height,
                crs=crs,
                transform=transform,
            ) as dst:
                dst.write(locals()[raster].astype(np.uint16), indexes=1)

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
        becvalue_cleaned,
        becvalue_resampled,
        src_transform=transform,
        dst_transform=dst_transform,
        src_crs=crs,
        dst_crs=crs,
        resampling=Resampling.nearest
    )

    # write to file
    results = (
            {'properties': {'becvalue': v}, 'geometry': s}
            for i, (s, v)
            in enumerate(
                shapes(becvalue_resampled, transform=dst_transform))
    )
    with fiona.open(
            becvalue, 'w',
            driver="GPKG",
            crs=crs,
            schema={'properties': [('becvalue', 'int')],
                    'geometry': 'Polygon'}) as dst:
        dst.writerecords(results)
    log.info("becmodel_tempdata/becvalue.gpkg created")
