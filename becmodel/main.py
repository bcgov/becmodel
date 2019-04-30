import os
import math
import logging
import shutil
from math import ceil

import fiona
import rasterio
from rasterio import features
from rasterio.features import shapes
from osgeo import gdal
import numpy as np
from skimage.filters.rank import majority
from skimage.morphology import rectangle, remove_small_objects
from skimage.measure import label

import bcdata
from becmodel import util
from becmodel.config import config


log = logging.getLogger(__name__)


def process(overwrite=False, qa=False):
    """ Generate becvalue file from rules and DEM
    """

    # load and validate inputs (rule polys, elevation table, becmaster)
    data = util.load_tables()

    # get bounds from gdf and align to Hectares BC raster
    bounds = util.align(list(data["rulepolys"].geometry.total_bounds))

    # confirm workspace exists, overwrite if specified
    if overwrite and os.path.exists(config["wksp"]):
        shutil.rmtree(config["wksp"])
    util.make_sure_path_exists(config["wksp"])

    # get dem, generate slope and aspect (these are always written to file)
    if not os.path.exists(os.path.join(config["wksp"], "dem.tif")):
        bcdata.get_dem(
            bounds,
            os.path.join(config["wksp"], "dem.tif"),
            resolution=config["cell_size"]
        )

    if not os.path.exists(os.path.join(config["wksp"], "slope.tif")):
        gdal.DEMProcessing(
            os.path.join(config["wksp"], "slope.tif"),
            os.path.join(config["wksp"], "dem.tif"),
            "slope",
            slopeFormat="percent"
        )

    if not os.path.exists(os.path.join(config["wksp"], "aspect.tif")):
        gdal.DEMProcessing(
            os.path.join(config["wksp"], "aspect.tif"),
            os.path.join(config["wksp"], "dem.tif"),
            "aspect",
            zeroForFlat=True
        )

    # load slope from file
    with rasterio.open(os.path.join(config["wksp"], "slope.tif")) as src:
        slope_image = src.read(1)

    # load and classify aspect
    with rasterio.open(os.path.join(config["wksp"], "aspect.tif")) as src:
        array1 = src.read(1)
        # set aspect to -1 for all slopes less that 15%
        array1[slope_image < config["flat_aspect_slope_threshold"]] = -1
        aspect_class = array1.copy()
        profile = src.profile
        for aspect in config["aspects"]:
            for rng in aspect["ranges"]:
                aspect_class[
                    (array1 >= rng["min"]) & (array1 < rng["max"])
                ] = aspect["code"]

    # load dem into memory and get the shape / transform
    # (so new rasters line up)
    with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:
        shape = src.shape
        transform = src.transform
        height = src.height
        width = src.width
        crs = src.crs
        dem_image = src.read(1)

    # burn rule polygon number to raster using DEM shape/transform
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
                (aspect_class == aspect["code"]) &
                (dem_image >= row[aspect["name"]+"_low"]) &
                (dem_image < row[aspect["name"]+"_high"])
            ] = row["becvalue"]

    # Smooth by applying majority filter to output
    # Note that skimage.filters.rank.majority is currently unreleased
    low_slope_size = ceil((config["majority_filter_low_slope_size"] / config["cell_size"]))
    steep_slope_size = ceil((config["majority_filter_steep_slope_size"] / config["cell_size"]))
    becvalue_filtered = np.where(
        slope_image < config["majority_filter_steep_slope_threshold"],
        majority(becvalue_image,
                 rectangle(width=low_slope_size, height=low_slope_size)),
        majority(becvalue_image,
                 rectangle(width=steep_slope_size, height=steep_slope_size))
    )

    # Remove areas smaller than noise removal threshold
    # first, convert noise_removal_threshold value from m2 to n cells
    noise_threshold = int(
        config["noise_removal_threshold"] / (config["cell_size"] **2)
    )

    # now find unique cell groupings (like converting to singlepart)
    becvalue_labels = label(becvalue_filtered, connectivity=1)

    # identify the areas smaller than noise removal threshold
    mask = remove_small_objects(becvalue_labels, noise_threshold)

    # Fill in the masked areas by again applying a majority filter.
    # But, this time,
    # - exclude areas smaller than noise threshold from majority filter calc
    # - only use the result to fill in the holes
    becvalue_cleaned = np.where(
        (mask == 0) & (becvalue_filtered > 0),
        majority(becvalue_filtered, rectangle(height=10, width=10), mask=mask),
        becvalue_filtered
    )

    # if specified, dump all intermediate rasters to disk for review
    if qa:
        for raster in [
            "becvalue_image",
            "becvalue_filtered",
            "becvalue_labels",
            "mask",
            "becvalue_cleaned",
            "rules_image",
            "aspect_class"
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

    # write to file
    results = (
            {"properties": {"becvalue": v}, "geometry": s}
            for i, (s, v)
            in enumerate(
                shapes(becvalue_cleaned, transform=transform))
    )
    with fiona.open(
            os.path.join(config["wksp"], config["out_file"]),
            "w",
            layer=config["out_layer"],
            driver="GPKG",
            crs=crs,
            schema={"properties": [("becvalue", "int")],
                    "geometry": "Polygon"}) as dst:
        dst.writerecords(results)
    log.info("Output {} created".format(os.path.join(config["wksp"], config["out_file"])))
