import os
import logging
import shutil
from math import ceil
import rasterio
from rasterio import features
from rasterio.features import shapes
from osgeo import gdal
import numpy as np
import pandas as pd
import geopandas as gpd
from geojson import Feature, FeatureCollection
from skimage.filters.rank import majority
import skimage.morphology as morphology
import bcdata

import becmodel
from becmodel import util


log = logging.getLogger(__name__)


class BECModel(object):
    """A class to hold a model's config, data and methods
    """

    def __init__(self, config_file=None):
        log.info("Initializing BEC model v{}".format(becmodel.__version__))
        self.config_file = config_file
        self.config = util.load_config(self.config_file)
        util.configure_logging(self.config)

    def update_config(self, update_dict):
        self.config.update(update_dict)

    def validate(self):
        self.data = util.load_tables(self.config)
        # arbitrarily assign grid raster values based on list of beclabels
        self.becvalue_lookup = {
            v: i
            for i, v in enumerate(
                list(self.data["elevation"].beclabel.unique()), start=1
            )
        }
        # create a reverse lookup
        self.beclabel_lookup = {
            value: key for key, value in self.becvalue_lookup.items()
        }
        # add zeros to reverse lookup
        self.beclabel_lookup[0] = None

    def get_merge_codes(self, rule_poly):
        """
        Given a rule polygon number, return a mapping of valid beclabel
        transition paths for high elevation removals
        """
        alpine = (
            self.data["elevation"]
            .beclabel[
                (self.data["elevation"].polygon_number == rule_poly)
                & (
                    self.data["elevation"]
                    .beclabel.str[:4]
                    .str.strip()
                    .isin(self.config["high_elevation_removal_threshold_alpine"])
                )
            ]
            .tolist()
        )

        parkland = (
            self.data["elevation"]
            .beclabel[
                (self.data["elevation"].polygon_number == rule_poly)
                & (
                    self.data["elevation"]
                    .beclabel.str[6:7]
                    .str.strip()
                    .isin(self.config["high_elevation_removal_threshold_parkland"])
                )
            ]
            .tolist()
        )

        woodland = (
            self.data["elevation"]
            .beclabel[
                (self.data["elevation"].polygon_number == rule_poly)
                & (
                    self.data["elevation"]
                    .beclabel.str[6:7]
                    .str.strip()
                    .isin(self.config["high_elevation_removal_threshold_woodland"])
                )
            ]
            .tolist()
        )
        # find beclabel used for high class by looking for substring of
        # woodland beclabel - only look for this if there is woodland present
        if woodland:
            high = (
                self.data["elevation"]
                .beclabel[
                    (self.data["elevation"].polygon_number == rule_poly)
                    & (
                        self.data["elevation"].beclabel.str[:7]
                        == woodland[0][:-1] + " "
                    )
                ]
                .tolist()
            )

        # Return a dict lookup defining the three merges, where present.
        # We presume that if a higher level is present, all lower levels
        # will be present as well
        merge_lookup = {}
        if alpine:
            merge_lookup[alpine[0]] = parkland[0]
        if parkland:
            merge_lookup[parkland[0]] = woodland[0]
        if woodland:
            merge_lookup[woodland[0]] = high[0]
        return merge_lookup

    def run(self, overwrite=False):
        """ Load input data, do all model calculations and filters
        """
        config = self.config
        self.validate()

        # shortcut
        data = self.data

        log.info("Downloading and processing DEM")

        # get bounds from gdf and bump out 2km
        bounds = list(data["rulepolys"].geometry.total_bounds)
        expansion = 2000
        xmin = bounds[0] - expansion
        ymin = bounds[1] - expansion
        xmax = bounds[2] + expansion
        ymax = bounds[3] + expansion
        expanded_bounds = (xmin, ymin, xmax, ymax)

        # align to Hectares BC raster
        data["bounds"] = util.align(expanded_bounds)

        # confirm workspace exists, overwrite if specified
        if overwrite and os.path.exists(config["wksp"]):
            shutil.rmtree(config["wksp"])
        util.make_sure_path_exists(config["wksp"])

        # ----------------------------------------------------------------
        # DEM processing
        # ----------------------------------------------------------------
        if not os.path.exists(os.path.join(config["wksp"], "dem.tif")):
            bcdata.get_dem(
                data["bounds"],
                os.path.join(config["wksp"], "dem.tif"),
                resolution=config["cell_size"],
            )

        # load dem into memory and get the shape / transform
        with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:
            shape = src.shape
            transform = src.transform
            data["dem"] = src.read(1)

        # generate slope and aspect (these are always written to file)
        if not os.path.exists(os.path.join(config["wksp"], "slope.tif")):
            gdal.DEMProcessing(
                os.path.join(config["wksp"], "slope.tif"),
                os.path.join(config["wksp"], "dem.tif"),
                "slope",
                slopeFormat="percent",
            )
        if not os.path.exists(os.path.join(config["wksp"], "aspect.tif")):
            gdal.DEMProcessing(
                os.path.join(config["wksp"], "aspect.tif"),
                os.path.join(config["wksp"], "dem.tif"),
                "aspect",
                zeroForFlat=True,
            )

        # load slope from file
        with rasterio.open(os.path.join(config["wksp"], "slope.tif")) as src:
            data["slope"] = src.read(1)

        # load and classify aspect
        with rasterio.open(os.path.join(config["wksp"], "aspect.tif")) as src:
            data["aspect"] = src.read(1)
            # set aspect to -1 for all slopes less that 15%
            data["aspect"][data["slope"] < config["flat_aspect_slope_threshold"]] = -1
            data["aspect_class"] = data["aspect"].copy()
            for aspect in config["aspects"]:
                for rng in aspect["ranges"]:
                    data["aspect_class"][
                        (data["aspect"] >= rng["min"]) & (data["aspect"] < rng["max"])
                    ] = aspect["code"]

        # ----------------------------------------------------------------
        # Iterate through rule polys, doing all processing within rule
        # polys independently
        # ----------------------------------------------------------------

        for rule_val in data["rulepolys"].polygon_number.tolist():
            log.info("Processing rule polygon {}".format(rule_val))
            data[rule_val] = {}
            # get raw rule polygon boundary
            data[rule_val]["01_rulepoly"] = data["rulepolys"][
                data["rulepolys"].polygon_number == rule_val
            ][["polygon_number", "geometry"]]

            # copy and buffer by 2km
            data[rule_val]["02_rulepolybuf"] = data[rule_val]["01_rulepoly"].copy()
            data[rule_val]["02_rulepolybuf"]["geometry"] = data[rule_val][
                "02_rulepolybuf"
            ].buffer(2000)

            # rasterize the buffered rulepoly
            data[rule_val]["03_ruleimg"] = features.rasterize(
                (
                    (geom, value)
                    for geom, value in zip(
                        data[rule_val]["02_rulepolybuf"].geometry,
                        data[rule_val]["02_rulepolybuf"].polygon_number,
                    )
                ),
                out_shape=shape,
                transform=transform,
                all_touched=False,
                dtype=np.uint16,
            )

            # ----------------------------------------------------------------
            # Generate initial becvalue raster
            # Generate becvalue raster by iterating through elevation table,
            # setting output raster to becvalue for each row where criteria are met
            # by the dem/aspect/rulepolys
            # ----------------------------------------------------------------
            log.info("Generating initial becvalue raster")
            data[rule_val]["04_becinit"] = np.zeros(shape=shape, dtype="uint16")
            for index, row in data["elevation"][
                data["elevation"].polygon_number == rule_val
            ].iterrows():
                for aspect in config["aspects"]:
                    data[rule_val]["04_becinit"][
                        (data[rule_val]["03_ruleimg"] == rule_val)
                        & (data["aspect_class"] == aspect["code"])
                        & (data["dem"] >= row[aspect["name"] + "_low"])
                        & (data["dem"] < row[aspect["name"] + "_high"])
                    ] = self.becvalue_lookup[row["beclabel"]]

            # ----------------------------------------------------------------
            # majority filter
            # ----------------------------------------------------------------
            log.info("Running majority filter")
            low_slope_size = ceil(
                (config["majority_filter_low_slope_size"] / config["cell_size"])
            )
            steep_slope_size = ceil(
                (config["majority_filter_steep_slope_size"] / config["cell_size"])
            )
            data[rule_val]["05_majority"] = np.where(
                data["slope"] < config["majority_filter_steep_slope_threshold"],
                majority(
                    data[rule_val]["04_becinit"],
                    morphology.rectangle(width=low_slope_size, height=low_slope_size),
                ),
                majority(
                    data[rule_val]["04_becinit"],
                    morphology.rectangle(
                        width=steep_slope_size, height=steep_slope_size
                    ),
                ),
            )

            # ----------------------------------------------------------------
            # noise removal
            # removing holes within each zone < the size threshold
            # ----------------------------------------------------------------
            log.info("Running noise removal filter")

            # convert noise_removal_threshold value from m2 to n cells
            noise_threshold = int(
                config["noise_removal_threshold"] / (config["cell_size"] ** 2)
            )
            # initialize the output raster for noise filter
            data[rule_val]["06_noise"] = np.zeros(shape=shape, dtype="uint16")

            # loop through all becvalues
            # (first removing the extra zero in the lookup)
            for becvalue in [v for v in self.beclabel_lookup if v != 0]:

                # extract given becvalue
                X = np.where(data[rule_val]["05_majority"] == becvalue, 1, 0)

                # fill holes, remove small objects
                Y = morphology.remove_small_holes(X, noise_threshold)
                Z = morphology.remove_small_objects(Y, noise_threshold)

                # insert values into output
                data[rule_val]["06_noise"] = np.where(
                    Z != 0, becvalue, data[rule_val]["06_noise"]
                )

            # above removes noise only where it is not on the edge.
            # Just in case small areas of noise remain on the edges, apply
            # area_closing function to remove
            data[rule_val]["07_areaclosing"] = data[rule_val]["06_noise"]

            # extract image area within the rule poly, set everything out to
            # a dummy value that should be outside the range of becvalues
            X = np.where(
                data[rule_val]["03_ruleimg"] == rule_val,
                data[rule_val]["06_noise"],
                225,
            )
            Y = morphology.area_closing(X, noise_threshold, connectivity=1)
            data[rule_val]["07_areaclosing"] = np.where(
                (data[rule_val]["03_ruleimg"] == rule_val)
                & (data[rule_val]["06_noise"] == 0),
                Y,
                data[rule_val]["07_areaclosing"],
            )

            # ----------------------------------------------------------------
            # High elevation noise removal
            # ----------------------------------------------------------------
            # convert high_elevation_removal_threshold value from m2 to n cells
            high_elevation_removal_threshold = int(
                self.config["high_elevation_removal_threshold"]
                / (self.config["cell_size"] ** 2)
            )

            # before processing the high elevation filter, build lookup
            # for transitioning zones in the rule poly
            merge_lookup = self.get_merge_codes(rule_val)
            if merge_lookup:
                log.info("Running high elevation minimum size filter")

                # initialize output
                data[rule_val]["08_highelev"] = np.where(
                    data[rule_val]["03_ruleimg"] == rule_val,
                    data[rule_val]["07_areaclosing"],
                    0,
                )

                # iterate through the merges (in order of insertion)
                for merge_label in merge_lookup:
                    log.info(
                        "high_elevation_removal: rule_poly: {}, merge_label:{}".format(
                            rule_val, merge_label
                        )
                    )

                    # Extract area of interest:
                    # if finding small alpine areas, extract parkland and
                    # remove small holes from the parkland
                    X = np.where(
                        data[rule_val]["08_highelev"]
                        == self.becvalue_lookup[merge_lookup[merge_label]],
                        1,
                        0,
                    )

                    # remove small holes from parkland/woodland/high
                    Y = morphology.remove_small_holes(
                        X, high_elevation_removal_threshold
                    )

                    # apply the removed holes to output image where value
                    # corresponds to value to be removed (ie, alpine for parkland)
                    data[rule_val]["08_highelev"] = np.where(
                        (Y == 1)
                        & (
                            data[rule_val]["08_highelev"]
                            == self.becvalue_lookup[merge_label]
                        ),
                        self.becvalue_lookup[merge_lookup[merge_label]],
                        data[rule_val]["08_highelev"],
                    )

            # convert to polygon
            fc = FeatureCollection(
                [
                    Feature(geometry=s, properties={"becvalue": v})
                    for i, (s, v) in enumerate(shapes(data[rule_val]["08_highelev"], transform=transform))
                ]
            )
            data[rule_val]["09_polybuf"] = gpd.GeoDataFrame.from_features(fc)

            # clip to original rule poly bnd
            data[rule_val]["10_polyclip"] = gpd.overlay(
                data[rule_val]["09_polybuf"],
                data[rule_val]["01_rulepoly"],
                how="intersection",
            )

        # load clipped output polygon dfs into a list and concatenate
        data["becvalue_1"] = pd.concat(
            [
                data[rule_val]["10_polyclip"]
                for rule_val in data["rulepolys"].polygon_number.tolist()
            ]
        ).pipe(gpd.GeoDataFrame)

        # dissolve the result
        data["becvalue"] = util.multi2single(
            data["becvalue_1"]
            .dissolve(by="becvalue")
            .reset_index()[["becvalue", "geometry"]]
        )

        # add output beclabel column
        data["becvalue"]["beclabel"] = data["becvalue"]["becvalue"].map(
            self.beclabel_lookup
        )

        self.data = data

    def write(self, qa=False):
        """ Write outputs to disk
        """
        config = self.config
        # read DEM to get crs / width / height etc
        with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:

            # write final output layer to file
            self.data["becvalue"].to_file(
                os.path.join(config["wksp"], config["out_file"]),
                layer=config["out_layer"],
                driver="GPKG",
            )

            log.info(
                "Output {} created".format(
                    os.path.join(config["wksp"], config["out_file"])
                )
            )

            # if specified, write all intermediate data to disk
            if qa:
                for rule_val in self.data["rulepolys"].polygon_number.tolist():
                    for raster in [
                        "03_ruleimg",
                        "04_becinit",
                        "05_majority",
                        "06_noise",
                        "07_areaclosing",
                        "08_highelev",
                    ]:
                        out_tif = os.path.join(
                            config["wksp"], "t{}_{}.tif".format(rule_val, raster)
                        )
                        with rasterio.open(
                            out_tif,
                            "w",
                            driver="GTiff",
                            dtype=rasterio.uint16,
                            count=1,
                            width=src.width,
                            height=src.height,
                            crs=src.crs,
                            transform=src.transform,
                        ) as dst:
                            dst.write(
                                self.data[rule_val][raster].astype(np.uint16), indexes=1
                            )
                for rule_val in self.data["rulepolys"].polygon_number.tolist():
                    for lyr in [
                        "01_rulepoly",
                        "02_rulepolybuf",
                        "09_polybuf",
                        "10_polyclip",
                    ]:
                        self.data[rule_val][lyr].to_file(
                            os.path.join(config["wksp"], config["out_file"]),
                            layer="t{}_{}".format(rule_val, lyr),
                            driver="GPKG",
                        )
                self.data["becvalue_1"].to_file(
                    os.path.join(config["wksp"], config["out_file"]),
                    layer="becvalue_1".format(rule_val, lyr),
                    driver="GPKG",
                )
