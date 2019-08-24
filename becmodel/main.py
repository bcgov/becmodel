import configparser
import os
import logging
import shutil
from math import ceil
import fiona
import rasterio
from rasterio import features
from rasterio.features import shapes
from osgeo import gdal
import numpy as np
import geopandas as gpd
from geojson import Feature, FeatureCollection
from skimage.filters.rank import majority, mean
import skimage.morphology as morphology

import bcdata

import becmodel
from becmodel import util
from becmodel.config import defaultconfig


log = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


class BECModel(object):
    """A class to hold a model's config, data and methods
    """

    def __init__(self, config_file=None):
        log.info("Initializing BEC model v{}".format(becmodel.__version__))

        # load and create local copy of default config
        self.config = defaultconfig.copy()

        # copy config[temp_folder] value to config[wksp] for brevity
        self.config["wksp"] = self.config["temp_folder"]

        # load and validate supplied config file
        if config_file:
            self.read_config(config_file)
            self.validate_config()
        util.configure_logging(self.config)

        # load inputs & validate
        self.data = util.load_tables(self.config)

    def read_config(self, config_file):
        """Read provided config file, overwriting default config values
        """
        log.info("Loading config from file: %s", config_file)
        cfg = configparser.ConfigParser()
        cfg.read(config_file)
        cfg_dict = dict(cfg["CONFIG"])

        for key in cfg_dict:
            if key not in self.config.keys():
                raise ConfigError("Config key {} is invalid".format(key))
            self.config[key] = cfg_dict[key]

        # convert int config values to int
        for key in [
            "cell_size_metres",
            "cell_connectivity",
            "noise_removal_threshold_ha",
            "high_elevation_removal_threshold_ha",
            "aspect_neutral_slope_threshold_percent",
            "aspect_midpoint_cool_degrees",
            "aspect_midpoint_neutral_east_degrees",
            "aspect_midpoint_warm_degrees",
            "aspect_midpoint_neutral_west_degrees",
            "majority_filter_steep_slope_threshold_percent",
            "majority_filter_size_slope_low_metres",
            "majority_filter_size_slope_steep_metres",
            "expand_bounds_metres",
        ]:
            self.config[key] = int(self.config[key])
        self.config["config_file"] = config_file
        self.config["wksp"] = self.config["temp_folder"]

    def update_config(self, update_dict, reload=False):
        """Update config dictionary, reloading source data if specified
        """
        self.config.update(update_dict)
        # set config temp_folder to wksp for brevity
        if "temp_folder" in update_dict.keys():
            self.config["wksp"] = update_dict["temp_folder"]
        self.validate_config()
        if reload:
            self.data = util.load_tables(self.config)

    def validate_config(self):
        """Validate provided config and add aspect temp zone definitions
        """
        # validate that required paths exist
        for key in ["rulepolys_file", "elevation"]:
            if not os.path.exists(self.config[key]):
                raise ConfigValueError(
                    "config {}: {} does not exist".format(key, self.config[key])
                )

        # validate rule polygon layer exists
        if self.config["rulepolys_layer"] and self.config[
            "rulepolys_layer"
        ] not in fiona.listlayers(self.config["rulepolys_file"]):
            raise ConfigValueError(
                "config {}: {} does not exist in {}".format(
                    key, self.config["rulepolys_layer"], self.config["rulepolys_file"]
                )
            )
        # for alignment to work, cell size must be <= 100m
        if (
            self.config["cell_size_metres"] < 25
            or self.config["cell_size_metres"] > 100
            or self.config["cell_size_metres"] % 5 != 0
        ):
            raise ConfigValueError(
                "cell size {} invalid - must be a multiple of 5 from 25-100".format(
                    str(self.config["cell_size_metres"])
                )
            )
        # convert True/False config values to boolean type
        for key in self.config:
            if self.config[key] in ["True", "False"]:
                self.config[key] = self.config[key] == "True"

        # define aspect zone codes and positions (1=cool, 2=neutral, 3=warm)
        self.aspect_zone_codes = [1, 2, 3, 2, 1]

        # load configured aspect temp zone aspect midpoint values, creating a
        # a ordered list defining aspect value of midpoint of each zone.
        # [cool, neutral_east, warm, neutral_west, cool]
        self.aspect_zone_midpoints = [
            self.config["aspect_midpoint_cool_degrees"],
            self.config["aspect_midpoint_neutral_east_degrees"],
            self.config["aspect_midpoint_warm_degrees"],
            self.config["aspect_midpoint_neutral_west_degrees"],
            self.config["aspect_midpoint_cool_degrees"],
        ]

        # Now we can create another list holding the differences (in degrees
        # aspect) between each zone
        self.aspect_zone_differences = list(
            np.mod(np.diff(np.array(self.aspect_zone_midpoints)), 360)
        )

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
                    high_elevation_dissolves[highelev_type].append(
                        mergerule["becvalue"]
                    )
                if mergerule["type"] == "woodland":
                    high_elevation_dissolves["high"].append(
                        mergerule["becvalue_target"]
                    )

        # remove any duplicates
        for highelev_type in ["alpine", "parkland", "woodland", "high"]:
            high_elevation_dissolves[highelev_type] = list(
                set(high_elevation_dissolves[highelev_type])
            )

        return high_elevation_dissolves

    def load(self, overwrite=False):
        """ Load input data, do all model calculations and filters
        """
        # shortcuts
        config = self.config
        data = self.data

        # arbitrarily assign grid raster values based on list of beclabels
        self.becvalue_lookup = {
            v: i
            for i, v in enumerate(list(data["elevation"].beclabel.unique()), start=1)
        }
        # create a reverse lookup
        self.beclabel_lookup = {
            value: key for key, value in self.becvalue_lookup.items()
        }
        # add zeros to reverse lookup
        self.beclabel_lookup[0] = None

        # convert slope dependent filter sizes from m to cells
        self.filtersize_low = ceil(
            (
                config["majority_filter_size_slope_low_metres"]
                / config["cell_size_metres"]
            )
        )
        self.filtersize_steep = ceil(
            (
                config["majority_filter_size_slope_steep_metres"]
                / config["cell_size_metres"]
            )
        )

        log.info("Downloading and processing DEM")

        # get bounds from gdf and bump out by specified expansion
        bounds = list(data["rulepolys"].geometry.total_bounds)
        xmin = bounds[0] - config["expand_bounds_metres"]
        ymin = bounds[1] - config["expand_bounds_metres"]
        xmax = bounds[2] + config["expand_bounds_metres"]
        ymax = bounds[3] + config["expand_bounds_metres"]
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
                resolution=config["cell_size_metres"],
            )

        # load dem into memory and get the shape / transform
        with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:
            self.shape = src.shape
            self.transform = src.transform
            data["dem"] = src.read(1)

        # because slope and aspect are always derived from a file DEM,
        # provide an option to pre-filter the DEM used for aspect generation
        if config["dem_prefilter"] is True:
            aspect_dem = os.path.join(config["wksp"], "dem_filtered.tif")
            data["dem_filtered"] = mean(
                data["dem"].astype(np.uint16), morphology.disk(3)
            )
            with rasterio.open(
                aspect_dem,
                "w",
                driver="GTiff",
                dtype=rasterio.uint16,
                count=1,
                width=src.width,
                height=src.height,
                crs=src.crs,
                transform=src.transform,
            ) as dst:
                dst.write(self.data["dem_filtered"], indexes=1)
        else:
            aspect_dem = os.path.join(config["wksp"], "dem.tif")

        # generate slope and aspect
        if not os.path.exists(os.path.join(config["wksp"], "slope.tif")):
            gdal.DEMProcessing(
                os.path.join(config["wksp"], "slope.tif"),
                os.path.join(config["wksp"], "dem.tif"),
                "slope",
                slopeFormat="percent",
            )
        if not os.path.exists(os.path.join(config["wksp"], "aspect.tif")):
            gdal.DEMProcessing(
                os.path.join(config["wksp"], "aspect.tif"), aspect_dem, "aspect"
            )

        # load slope from file
        with rasterio.open(os.path.join(config["wksp"], "slope.tif")) as src:
            data["slope"] = src.read(1)

        # load aspect and convert to unsigned integer
        with rasterio.open(os.path.join(config["wksp"], "aspect.tif")) as src:
            data["aspect"] = src.read(1).astype(np.uint16)

        # We consider slopes less that 15% to be neutral.
        # Set aspect to aspect_midpoint_neutral_east (ie, typically 90 degrees)
        data["aspect"][
            data["slope"] < config["aspect_neutral_slope_threshold_percent"]
        ] = config["aspect_midpoint_neutral_east_degrees"]

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
            out_shape=self.shape,
            transform=self.transform,
            all_touched=False,
            dtype=np.uint16,
        )
        self.data = data

    def model(self):
        """
        Generate initial becvalue raster.

        Create the raster by iterating through elevation table,
        setting output raster to becvalue for each row where criteria are met
        by the dem/aspect/rulepolys.
        """
        log.info("Generating initial becvalue raster")
        # shortcut
        data = self.data

        # Create the initial bec model
        # We assign beclabels based on elevation / aspect / rule polygon.
        # Elevations in the source elevation table are stretched across
        # the aspect temperature zones (cool/neutral/warm/neutral/cool) in
        # an effort to smooth out transitions values between aspects
        data["04_becinit"] = np.zeros(shape=self.shape, dtype="uint16")
        # iterate through rows in elevation table
        for index, row in data["elevation"].iterrows():
            # extract the low/high elevation values for each aspect temp zone
            cool = (row["cool_low"], row["cool_high"])
            neutral = (row["neutral_low"], row["neutral_high"])
            warm = (row["warm_low"], row["warm_high"])

            # define the four transitions, and iterate through them in
            # clockwise direction
            for i, transition in enumerate(
                [(cool, neutral), (neutral, warm), (warm, neutral), (neutral, cool)]
            ):
                # calculate elevation step size (m) per degree
                low_elev_step_size = (
                    transition[1][0] - transition[0][0]
                ) / self.aspect_zone_differences[i]

                high_elev_step_size = (
                    transition[1][1] - transition[0][1]
                ) / self.aspect_zone_differences[i]

                # make 10 degree steps through each transition, essentially
                # classifying aspect in 10 degree steps
                for step in range(0, self.aspect_zone_differences[i], 10):
                    aspect_min = ((self.aspect_zone_midpoints[i] + step) - 5) % 360
                    aspect_max = ((self.aspect_zone_midpoints[i] + step) + 5) % 360
                    elev_min = transition[0][0] + int(
                        round((step * low_elev_step_size))
                    )
                    elev_max = transition[0][1] + int(
                        round((step * high_elev_step_size))
                    )

                    # for any aspect classes where min > max (they span 0),
                    # do the < 360/0 part as a separate first step
                    if aspect_min > aspect_max:
                        data["04_becinit"][
                            (data["03_ruleimg"] == row["polygon_number"])
                            & (data["aspect"] >= aspect_min)
                            & (data["dem"] >= elev_min)
                            & (data["dem"] < elev_max)
                        ] = self.becvalue_lookup[row["beclabel"]]
                        # now start at zero
                        aspect_min = 0

                    # assign becvalues based on rule & min/max elev/aspect
                    data["04_becinit"][
                        (data["03_ruleimg"] == row["polygon_number"])
                        & (data["aspect"] >= aspect_min)
                        & (data["aspect"] < aspect_max)
                        & (data["dem"] >= elev_min)
                        & (data["dem"] < elev_max)
                    ] = self.becvalue_lookup[row["beclabel"]]

        self.data = data

    def postfilter(self):
        """ Tidy the output bec zones by applying several filters:
        - majority
        - noise
        - area closing (fill in 0 areas created by noise filter)
        - majority (again) to tidy edge effects created by area_closing()
        - noise (again) to remove any noise created by 2nd majority
        """
        # shortcuts
        config = self.config
        data = self.data

        # ----------------------------------------------------------------
        # majority filter
        # ----------------------------------------------------------------
        log.info("Running majority filter")
        data["05_majority"] = np.where(
            data["slope"] < config["majority_filter_steep_slope_threshold_percent"],
            majority(
                data["04_becinit"],
                morphology.rectangle(
                    width=self.filtersize_low, height=self.filtersize_low
                ),
            ),
            majority(
                data["04_becinit"],
                morphology.rectangle(
                    width=self.filtersize_steep, height=self.filtersize_steep
                ),
            ),
        )

        # ----------------------------------------------------------------
        # Noise Removal 1 - noisefilter
        # Remove holes < the noise_removal_threshold within each zone
        # ----------------------------------------------------------------
        log.info("Running noise removal filter")

        # convert noise_removal_threshold value from ha to n cells
        noise_threshold = int(
            (config["noise_removal_threshold_ha"] * 10000)
            / (config["cell_size_metres"] ** 2)
        )

        # initialize the output raster for noise filter
        data["06_noise"] = np.zeros(shape=self.shape, dtype="uint16")

        # loop through all becvalues
        # (first removing the extra zero in the lookup)
        for becvalue in [v for v in self.beclabel_lookup if v != 0]:

            # extract given becvalue
            X = np.where(data["05_majority"] == becvalue, 1, 0)

            # fill holes, remove small objects
            # fill holes, remove small objects
            Y = morphology.remove_small_holes(
                X, noise_threshold, connectivity=config["cell_connectivity"]
            )
            Z = morphology.remove_small_objects(
                Y, noise_threshold, connectivity=config["cell_connectivity"]
            )

            # insert values into output
            data["06_noise"] = np.where(Z != 0, becvalue, data["06_noise"])

        # ----------------------------------------------------------------
        # Noise Removal 2 - areaclosing
        # Noise on edges of rule polygons is introduced with above process
        # (removing small holes and then removing small objects leaves holes
        # of 0 along rule poly edges)
        # ----------------------------------------------------------------
        log.info("Running morphology.area_closing() to clean results of noise filter")
        data["07_areaclosing"] = data["06_noise"].copy()
        for rule_poly in data["rulepolys"].polygon_number.tolist():
            # extract image area within the rule poly
            X = np.where(data["03_ruleimg"] == rule_poly, data["06_noise"], 100)
            Y = morphology.area_closing(
                X, noise_threshold, connectivity=config["cell_connectivity"]
            )
            data["07_areaclosing"] = np.where(
                (data["03_ruleimg"] == rule_poly) & (data["06_noise"] == 0),
                Y,
                data["07_areaclosing"],
            )

        # ----------------------------------------------------------------
        # Noise Removal 3 - highelevfilter
        # High elevation noise removal
        # ----------------------------------------------------------------
        # initialize output image
        data["08_highelev"] = data["07_areaclosing"].copy()
        # convert high_elevation_removal_threshold value from ha to n cells
        high_elevation_removal_threshold = int(
            (self.config["high_elevation_removal_threshold_ha"] * 10000)
            / (self.config["cell_size_metres"] ** 2)
        )

        # Because we are finding noise by aggregating and finding holes,
        # iterate through all but the first high elevation type.
        # We presume that if a higher type is present, all below types are as
        # well (eg, if parkland is present, woodland and high must be too)
        high_elevation_types = list(self.high_elevation_dissolves.keys())
        for i, highelev_type in enumerate(high_elevation_types[:-1]):
            log.info(
                "Running high_elevation_removal_threshold on {}".format(highelev_type)
            )

            # Extract area of interest
            # eg, find and aggregate all parkland values for finding alpine
            # area < threshold
            to_agg = self.high_elevation_dissolves[high_elevation_types[i + 1]]
            X = np.isin(data["08_highelev"], to_agg)
            Y = morphology.remove_small_holes(
                X,
                high_elevation_removal_threshold,
                connectivity=config["cell_connectivity"],
            )

            # remove the small areas in the output image by looping through
            # the merges for the given type, this iterates through the
            # rule polygons.
            for merge in [
                m for m in self.high_elevation_merges if m["type"] == highelev_type
            ]:
                data["08_highelev"] = np.where(
                    (Y == 1) & (data["03_ruleimg"] == merge["rule"]),
                    merge["becvalue_target"],
                    data["08_highelev"],
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
            data["slope"] < config["majority_filter_steep_slope_threshold_percent"],
            majority(
                data["08_highelev"],
                morphology.rectangle(
                    width=self.filtersize_low, height=self.filtersize_low
                ),
            ),
            majority(
                data["08_highelev"],
                morphology.rectangle(
                    width=self.filtersize_steep, height=self.filtersize_steep
                ),
            ),
        )

        # ----------------------------------------------------------------
        # repeat noise filter
        # Repeating the majority filter can leave small amounts of residual
        # noise, run a basic noise filter
        # ----------------------------------------------------------------
        # initialize the output raster for noise filter
        log.info("Running noise filter again to clean results of majority filter")
        data["10_noise2"] = data["09_majority2"].copy()

        # loop through all becvalues
        # (first removing the extra zero in the lookup)
        for becvalue in [v for v in self.beclabel_lookup if v != 0]:

            # extract given becvalue
            X = np.where(data["09_majority2"] == becvalue, 1, 0)

            # fill holes, remove small objects
            Y = morphology.remove_small_holes(
                X, noise_threshold, connectivity=config["cell_connectivity"]
            )
            Z = morphology.remove_small_objects(
                Y, noise_threshold, connectivity=config["cell_connectivity"]
            )

            # insert values into output
            data["10_noise2"] = np.where(Z != 0, becvalue, data["10_noise2"])

        # ----------------------------------------------------------------
        # Convert to poly
        # ----------------------------------------------------------------
        fc = FeatureCollection(
            [
                Feature(geometry=s, properties={"becvalue": v})
                for i, (s, v) in enumerate(
                    shapes(
                        data["10_noise2"],
                        transform=self.transform,
                        connectivity=(config["cell_connectivity"] * 4),
                    )
                )
            ]
        )
        data["becvalue_polys"] = gpd.GeoDataFrame.from_features(fc)

        # add beclabel column to output polygons
        data["becvalue_polys"]["beclabel"] = data["becvalue_polys"]["becvalue"].map(
            self.beclabel_lookup
        )

        self.data = data

    def write(self, qa=False):
        """ Write outputs to disk
        """
        config = self.config
        # read DEM to get crs / width / height etc
        with rasterio.open(os.path.join(config["wksp"], "dem.tif")) as src:

            if qa:
                for raster in [
                    "03_ruleimg",
                    "04_becinit",
                    "05_majority",
                    "06_noise",
                    "07_areaclosing",
                    "08_highelev",
                    "09_majority2",
                    "10_noise2",
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
                config["out_file"], layer=config["out_layer"], driver="GPKG"
            )

            log.info("Output {} created".format(config["out_file"]))
