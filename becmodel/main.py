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

    @property
    def high_elevation_merges(self):
        """
        Return a list of dicts, defining what beclabel transition paths for
        high elevation noise removal.

        eg:

        [{'rule': 123, 'type': 'alpine', 'becvalue': 1, 'becvalue_target': 2},
         {'rule': 123, 'type': 'parkland', 'becvalue': 2, 'becvalue_target': 3},
         {'rule': 123, 'type': 'woodland', 'becvalue': 3, 'becvalue_target': 4},
         {'rule': 124, 'type': 'alpine', 'becvalue': 1, 'becvalue_target': 10},
         {'rule': 124, 'type': 'parkland', 'becvalue': 10, 'becvalue_target': 11},
         {'rule': 124, 'type': 'woodland', 'becvalue': 11, 'becvalue_target': 12},
         {'rule': 122, 'type': 'alpine', 'becvalue': 1, 'becvalue_target': 2},

        """
        high_elevation_merges = []
        for rule_poly in self.data["rulepolys"].polygon_number.tolist():

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

            # Translate the beclabels into becvalue integers,
            # and write each lookup to the list for the given rule poly
            if alpine:
                lookup = {
                    "rule": rule_poly,
                    "type": "alpine",
                    "becvalue": self.becvalue_lookup[alpine[0]],
                    "becvalue_target": self.becvalue_lookup[parkland[0]],
                }
                high_elevation_merges.append(lookup)

            if parkland:
                lookup = {
                    "rule": rule_poly,
                    "type": "parkland",
                    "becvalue": self.becvalue_lookup[parkland[0]],
                    "becvalue_target": self.becvalue_lookup[woodland[0]],
                }
                high_elevation_merges.append(lookup)

            if woodland:
                lookup = {
                    "rule": rule_poly,
                    "type": "woodland",
                    "becvalue": self.becvalue_lookup[woodland[0]],
                    "becvalue_target": self.becvalue_lookup[high[0]],
                }
                high_elevation_merges.append(lookup)

        return high_elevation_merges

    @property
    def high_elevation_dissolves(self):
        """
        Parse the high elevation merge rules to determine becvalues
        for each high elevation zone, required for dissolving across
        rule polygons. Return dict of lists, eg:

        {
            'alpine': [1],
            'parkland': [2, 10],
            'woodland': [11, 3],
            'high': [4, 12]
        }

        Note that we only have to supply one level of dissolves because we
        iterate through these in order. For example, when aggregating
        woodland, only the actual woodland codes are needed because any small
        holes in parkland are already been removed in the previous steps.
        """
        high_elevation_dissolves = {}
        high_elevation_dissolves["alpine"] = []
        high_elevation_dissolves["parkland"] = []
        high_elevation_dissolves["woodland"] = []
        high_elevation_dissolves["high"] = []

        for mergerule in self.high_elevation_merges:
            for highelev_type in high_elevation_dissolves:
                if mergerule["type"] == highelev_type:
                    high_elevation_dissolves[highelev_type].append(mergerule["becvalue"])
                if mergerule["type"] == "woodland":
                    high_elevation_dissolves["high"].append(mergerule["becvalue_target"])

        # remove any duplicates
        for highelev_type in ["alpine", "parkland", "woodland", "high"]:
            high_elevation_dissolves[highelev_type] = list(set(high_elevation_dissolves[highelev_type]))

        return high_elevation_dissolves

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
            data["02_aspectclass"] = data["aspect"].copy()
            for aspect in config["aspects"]:
                for rng in aspect["ranges"]:
                    data["02_aspectclass"][
                        (data["aspect"] >= rng["min"]) & (data["aspect"] < rng["max"])
                    ] = aspect["code"]

        # ----------------------------------------------------------------
        # rule polygons to raster
        # ----------------------------------------------------------------
        data["03_ruleimg"] = features.rasterize(
            (
                (geom, value)
                for geom, value in zip(
                    data["rulepolys"].geometry, data["rulepolys"].polygon_number
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
        data["04_becinit"] = np.zeros(shape=shape, dtype="uint16")
        for index, row in data["elevation"].iterrows():
            for aspect in config["aspects"]:
                data["04_becinit"][
                    (data["03_ruleimg"] == row["polygon_number"])
                    & (data["02_aspectclass"] == aspect["code"])
                    & (data["dem"] >= row[aspect["name"] + "_low"])
                    & (data["dem"] < row[aspect["name"] + "_high"])
                ] = self.becvalue_lookup[row["beclabel"]]

        log.info("Running majority filter")
        low_slope_size = ceil(
            (config["majority_filter_low_slope_size"] / config["cell_size"])
        )
        steep_slope_size = ceil(
            (config["majority_filter_steep_slope_size"] / config["cell_size"])
        )

        # ----------------------------------------------------------------
        # majority filter
        # ----------------------------------------------------------------
        data["05_majority"] = np.where(
            data["slope"] < config["majority_filter_steep_slope_threshold"],
            majority(
                data["04_becinit"],
                morphology.rectangle(width=low_slope_size, height=low_slope_size),
            ),
            majority(
                data["04_becinit"],
                morphology.rectangle(width=steep_slope_size, height=steep_slope_size),
            ),
        )

        # ----------------------------------------------------------------
        # Noise Removal 1 - noisefilter
        # Remove holes < the noise_removal_threshold within each zone
        # ----------------------------------------------------------------
        log.info("Running noise removal filter")

        # convert noise_removal_threshold value from m2 to n cells
        noise_threshold = int(
            config["noise_removal_threshold"] / (config["cell_size"] ** 2)
        )

        # initialize the output raster for noise filter
        data["06_noise"] = np.zeros(shape=shape, dtype="uint16")

        # loop through all becvalues
        # (first removing the extra zero in the lookup)
        for becvalue in [v for v in self.beclabel_lookup if v != 0]:

            # extract given becvalue
            X = np.where(data["05_majority"] == becvalue, 1, 0)

            # fill holes, remove small objects
            Y = morphology.remove_small_holes(X, noise_threshold)
            Z = morphology.remove_small_objects(Y, noise_threshold)

            # insert values into output
            data["06_noise"] = np.where(
                Z != 0, becvalue, data["06_noise"]
            )

        # ----------------------------------------------------------------
        # Noise Removal 2 - areaclosing
        # Noise on edges of rule polygons is introduced with above process
        # (removing small holes and then removing small objects leaves holes
        # of 0 along rule poly edges)
        # ----------------------------------------------------------------
        log.info("Cleaning noise filter results with morphology.area_closing()")
        data["07_areaclosing"] = data["06_noise"].copy()
        for rule_poly in data["rulepolys"].polygon_number.tolist():
            # extract image area within the rule poly
            X = np.where(
                data["03_ruleimg"] == rule_poly,
                data["06_noise"],
                100
            )
            Y = morphology.area_closing(X, noise_threshold, connectivity=1)
            data["07_areaclosing"] = np.where(
                (data["03_ruleimg"] == rule_poly) &
                (data["06_noise"] == 0),
                Y,
                data["07_areaclosing"]
            )

        # ----------------------------------------------------------------
        # Noise Removal 3 - highelevfilter
        # High elevation noise removal
        # ----------------------------------------------------------------
        # initialize output image
        data["08_highelev"] = data["07_areaclosing"].copy()
        # convert high_elevation_removal_threshold value from m2 to n cells
        high_elevation_removal_threshold = int(
            self.config["high_elevation_removal_threshold"]
            / (self.config["cell_size"] ** 2))

        # Because we are finding noise by aggregating and finding holes,
        # iterate through all but the first high elevation type.
        # We presume that if a higher type is present, all below types are as
        # well (eg, if parkland is present, woodland and high must be too)
        high_elevation_types = list(self.high_elevation_dissolves.keys())
        for i, highelev_type in enumerate(high_elevation_types[:-1]):
            log.info("Aggregating {} to remove {} under high_elevation_removal_threshold".format(high_elevation_types[i + 1], highelev_type))

            # Extract area of interest
            # eg, find and aggregate all parkland values for finding alpine
            # area < threshold
            to_agg = self.high_elevation_dissolves[high_elevation_types[i + 1]]
            X = np.isin(data["08_highelev"], to_agg)
            Y = morphology.remove_small_holes(X, high_elevation_removal_threshold)

            # remove the small areas in the output image by looping through
            # the merges for the given type, this iterates through the
            # rule polygons.
            for merge in [m for m in self.high_elevation_merges if m["type"] == highelev_type]:
                data["08_highelev"] = np.where(
                    (Y == 1) & (data["03_ruleimg"] == merge["rule"]),
                    merge["becvalue_target"],
                    data["08_highelev"]
                )

        # ----------------------------------------------------------------
        # repeat majority filter
        # Because the first majority filter slightly reshapes the effective
        # edges of the rule polys, using the source rule poly raster in the
        # intermediate steps introduces small noise on the edges. Run
        # another pass of the majority filter to clean this up. A single size
        # kernel would probably be fine but lets use the existing variable
        # size.
        # ----------------------------------------------------------------
        log.info("Running majority filter again to tidy edges")
        data["09_majority2"] = np.where(
            data["slope"] < config["majority_filter_steep_slope_threshold"],
            majority(
                data["08_highelev"],
                morphology.rectangle(width=low_slope_size, height=low_slope_size),
            ),
            majority(
                data["08_highelev"],
                morphology.rectangle(width=steep_slope_size, height=steep_slope_size),
            ),
        )

        # ----------------------------------------------------------------
        # repeat noise filter
        # Repeating the majority filter can leave small amounts of residual
        # noise, run a basic noise filter
        # ----------------------------------------------------------------
        # initialize the output raster for noise filter
        log.info("Removing any noise introduced by majority filter")
        data["10_noise2"] = data["09_majority2"].copy()

        # loop through all becvalues
        # (first removing the extra zero in the lookup)
        for becvalue in [v for v in self.beclabel_lookup if v != 0]:

            # extract given becvalue
            X = np.where(data["09_majority2"] == becvalue, 1, 0)

            # fill holes, remove small objects
            Y = morphology.remove_small_holes(X, noise_threshold)
            Z = morphology.remove_small_objects(Y, noise_threshold)

            # insert values into output
            data["10_noise2"] = np.where(
                Z != 0, becvalue, data["10_noise2"]
            )

        # ----------------------------------------------------------------
        # Convert to poly
        # ----------------------------------------------------------------
        fc = FeatureCollection(
            [
                Feature(geometry=s, properties={"becvalue": v})
                for i, (s, v) in enumerate(shapes(data["10_noise2"], transform=transform))
            ]
        )
        data["becvalue_polys"] = gpd.GeoDataFrame.from_features(fc)

        # add beclabel column to output polygons
        data["becvalue_polys"]['beclabel'] = data["becvalue_polys"]["becvalue"].map(self.beclabel_lookup)

        self.data = data

    def write(self, qa=False):
        """ Write outputs to disk
        """
        config = self.config
        # read DEM to get crs / width / height etc
        with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:

            if qa:
                for raster in [
                    "02_aspectclass",
                    "03_ruleimg",
                    "04_becinit",
                    "05_majority",
                    "06_noise",
                    "07_areaclosing",
                    "08_highelev",
                    "09_majority2",
                    "10_noise2"

                ]:
                    with rasterio.open(
                        os.path.join(config["wksp"], raster + ".tif"),
                        "w",
                        driver="GTiff",
                        dtype=rasterio.uint16,
                        count=1,
                        width=src.width,
                        height=src.height,
                        crs=src.crs,
                        transform=src.transform,
                    ) as dst:
                        dst.write(self.data[raster].astype(np.uint16), indexes=1)

            # write output vectors to file
            self.data["becvalue_polys"].to_file(
                os.path.join(config["wksp"], config["out_file"]),
                layer=config["out_layer"],
                driver="GPKG",
            )

            log.info(
                "Output {} created".format(
                    os.path.join(config["wksp"], config["out_file"])
                )
            )
