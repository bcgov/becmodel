import configparser
import os
from pathlib import Path
import logging
import shutil
from math import ceil
from datetime import datetime
import fiona
import rasterio
from rasterio import features
from rasterio.features import shapes
from rasterio.warp import transform_bounds
from rasterio.merge import merge as riomerge
from osgeo import gdal
import numpy as np
import geopandas as gpd
from geojson import Feature, FeatureCollection
from skimage.filters.rank import majority
import skimage.morphology as morphology
import click
from scipy import ndimage
import subprocess

import bcdata
from terraincache import TerrainTiles

import becmodel
from becmodel import util
from becmodel.config import defaultconfig


LOG = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


class BECModel(object):
    """A class to hold a model's config, data and methods
    """

    def __init__(self, config_file=None):
        LOG.info("Initializing BEC model v{}".format(becmodel.__version__))

        # load and validate supplied config file
        if config_file:
            if not os.path.exists(config_file):
                raise ConfigValueError(f"File {config_file} does not exist")

            self.read_config(config_file)
            self.validate_config()
        else:
            self.config = defaultconfig.copy()

        # load inputs & validate
        self.data = util.load_tables(self.config)

        # note start time for config log time stamp
        self.start_time = datetime.now()

    def read_config(self, config_file):
        """Read provided config file, overwriting default config values
        """
        LOG.info("Loading config from file: %s", config_file)
        cfg = configparser.ConfigParser()
        cfg.read(config_file)
        self.user_config = dict(cfg["CONFIG"])

        # ensure all keys are valid
        for key in self.user_config:
            if key not in defaultconfig.keys():
                raise ConfigError("Config key {} is invalid".format(key))

        # convert configparser strings to int/list where required
        for key in defaultconfig:
            if key in self.user_config.keys():
                if type(defaultconfig[key]) == int:
                    self.user_config[key] = int(self.user_config[key])
                elif type(defaultconfig[key]) == list:
                    self.user_config[key] = self.user_config[key].split(",")

        # create config from default and provided file
        self.config = {**defaultconfig, **self.user_config}

        # add shortcut to temp folder
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

        # validate becmaster is not provided, use table provided in /data
        if not self.config["becmaster"]:
            self.config["becmaster"] = os.path.join(
                os.path.dirname(__file__), "data/bec_biogeoclimatic_catalogue.csv"
            )
        if not os.path.exists(self.config["becmaster"]):
            raise ConfigValueError(
                "BECMaster {} specified in config does not exist.".format(
                    self.config["becmaster"]
                )
            )
        # is DEM path provided? If so, validate file exists
        # (no validation that it actually overlaps the rule polygons, we
        # will presume that the user has that under control)
        if self.config["dem"]:
            if not os.path.exists(self.config["dem"]):
                raise ConfigValueError(
                    "DEM file {} specified in config does not exist.".format(
                        self.config["dem"]
                    )
                )

    def write_config_log(self):
        """dump configs to file"""
        configlog = configparser.ConfigParser()
        configlog["1_VERSION"] = {"becmodel_version": becmodel.__version__}

        configlog["2_USER"] = {}
        for key in self.user_config:
            # convert config values back to string
            if type(defaultconfig[key]) in (int, bool):
                configlog["2_USER"][key] = str(self.user_config[key])
            elif type(defaultconfig[key]) == list:
                configlog["2_USER"][key] = ",".join(self.user_config[key])
            else:
                configlog["2_USER"][key] = str(self.user_config[key])

        configlog["3_DEFAULT"] = {}
        for key in defaultconfig:
            if key not in self.user_config:
                if type(defaultconfig[key]) in (int, bool):
                    configlog["3_DEFAULT"][key] = str(defaultconfig[key])
                elif type(defaultconfig[key]) == list:
                    configlog["3_DEFAULT"][key] = ",".join(defaultconfig[key])
                else:
                    configlog["3_DEFAULT"][key] = str(defaultconfig[key])

        timestamp = self.start_time.isoformat(sep="T", timespec="seconds")
        # windows does not support ISO datestamps (:)
        timestamp = timestamp.replace(":", "-")
        config_log = f"becmodel-config-log_{timestamp}.txt"
        LOG.info(f"Logging config to here: {config_log}")
        with open(config_log, "w") as configfile:
            configlog.write(configfile)

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
                            == woodland[0][:-3] + " "
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

        # note workspace
        LOG.info("Temp data are here: {}".format(self.config["temp_folder"]))

        # create dict that maps beclabel to becvalue
        uniques = (
            data["elevation"][["beclabel", "becvalue"]]
            .drop_duplicates()
            .to_dict("list")
        )
        self.becvalue_lookup = {}
        for i, v in enumerate(uniques["beclabel"]):
            self.becvalue_lookup[v] = uniques["becvalue"][i]

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

        # get bounds from gdf and bump out by specified expansion
        bounds = list(data["rulepolys"].geometry.total_bounds)
        xmin = bounds[0] - config["expand_bounds_metres"]
        ymin = bounds[1] - config["expand_bounds_metres"]
        xmax = bounds[2] + config["expand_bounds_metres"]
        ymax = bounds[3] + config["expand_bounds_metres"]
        expanded_bounds = (xmin, ymin, xmax, ymax)

        # align to Hectares BC raster
        data["bounds"] = util.align(expanded_bounds)

        LOG.info("Bounds: " + " ".join([str(b) for b in data["bounds"]]))

        # confirm workspace exists, overwrite if specified
        if overwrite and os.path.exists(config["wksp"]):
            shutil.rmtree(config["wksp"])
        # create workspace and a subfolder for the non-numbered rasters
        # (so that they can be cached for repeated model runs on the same
        # study area)
        srcpath = os.path.join(config["wksp"], "src")
        Path(srcpath).mkdir(parents=True, exist_ok=True)

        # do bounds extend outside of BC?
        bounds_ll = transform_bounds("EPSG:3005", "EPSG:4326", *data["bounds"])
        bounds_gdf = util.bbox2gdf(bounds_ll)

        # load neighbours
        # Note that the natural earth dataset is only 1:10m,
        # buffer it by 2k to be sure it captures the edge of the province
        nbr = (
            gpd.read_file(
                os.path.join(os.path.dirname(__file__), "data/neighbours.geojson")
            )
            .dissolve(by="scalerank")
            .buffer(2000)
        )
        neighbours = (
            gpd.GeoDataFrame(nbr)
            .rename(columns={0: "geometry"})
            .set_geometry("geometry")
        )
        outside_bc = gpd.overlay(neighbours, bounds_gdf, how="intersection")

        # use file based dem if provided in config
        if config["dem"]:
            LOG.info("Using DEM: {}".format(config["dem"]))
            dempath = config["dem"]
        else:
            dempath = os.path.join(srcpath, "dem.tif")

        # We cache the result of WCS / terraintiles requests, so only
        # rerun if the file is not present
        dem_bc = os.path.join(srcpath, "dem_bc.tif")
        dem_exbc = os.path.join(srcpath, "dem_exbc.tif")
        if not config["dem"]:

            # get TRIM dem
            if not os.path.exists(dem_bc):
                LOG.info("Downloading and processing BC DEM")
                # request at native resolution and resample locally
                # because requesting a bilinear resampled DEM is slow
                bcdata.get_dem(
                    data["bounds"], os.path.join(srcpath, "dem_bc25.tif"), resolution=25
                )
                # resample if "cell_size_metres" is not 25m
                if config["cell_size_metres"] != 25:
                    LOG.info("Resampling BC DEM")
                    cmd = [
                        "gdalwarp",
                        "-r",
                        "bilinear",
                        "-tr",
                        str(config["cell_size_metres"]),
                        str(config["cell_size_metres"]),
                        os.path.join(srcpath, "dem_bc25.tif"),
                        dem_bc,
                    ]
                    subprocess.run(cmd)
                # otherwise, just rename
                else:
                    LOG.info("xxx")
                    os.rename(os.path.join(srcpath, "dem_bc25.tif"), dem_bc)

                # if not requesting terrain-tiles, again just rename the bc dem
                if outside_bc.empty is True:
                    LOG.info("yyy")
                    os.rename(dem_bc, dempath)
            # get terrain-tiles
            # - if the bbox does extend outside of BC
            # - if _exbc file is not already present
            if not os.path.exists(dem_exbc) and not outside_bc.empty:

                # find path to cached terrain-tiles
                if "TERRAINCACHE" in os.environ.keys():
                    terraincache_path = os.environ["TERRAINCACHE"]
                else:
                    terraincache_path = os.path.join(config["wksp"], "terrain-tiles")
                tt = TerrainTiles(
                    data["bounds"],
                    11,
                    cache_dir=terraincache_path,
                    bounds_crs="EPSG:3005",
                    dst_crs="EPSG:3005",
                    resolution=config["cell_size_metres"],
                )
                tt.save(out_file=dem_exbc)

                # combine the sources
                a = rasterio.open(dem_bc)
                b = rasterio.open(dem_exbc)
                mosaic, out_trans = riomerge([b, a])
                out_meta = a.meta.copy()
                out_meta.update(
                    {
                        "driver": "GTiff",
                        "height": mosaic.shape[1],
                        "width": mosaic.shape[2],
                        "transform": out_trans,
                        "crs": "EPSG:3005",
                    }
                )
                # write merged tiff
                with rasterio.open(dempath, "w", **out_meta) as dest:
                    dest.write(mosaic)

        # ----------------------------------------------------------------
        # DEM processing
        # ----------------------------------------------------------------
        # load dem into memory and get the shape / transform
        with rasterio.open(dempath) as src:
            self.shape = src.shape
            self.transform = src.transform
            data["dem"] = src.read(1)

        # generate slope and aspect
        if not os.path.exists(os.path.join(srcpath, "slope.tif")):
            gdal.DEMProcessing(
                os.path.join(srcpath, "slope.tif"),
                dempath,
                "slope",
                slopeFormat="percent",
            )
        if not os.path.exists(os.path.join(srcpath, "aspect.tif")):
            gdal.DEMProcessing(os.path.join(srcpath, "aspect.tif"), dempath, "aspect")

        # load slope from file
        with rasterio.open(os.path.join(srcpath, "slope.tif")) as src:
            data["slope"] = src.read(1)

        # load aspect and convert to unsigned integer
        with rasterio.open(os.path.join(srcpath, "aspect.tif")) as src:
            data["aspect"] = src.read(1).astype(np.uint16)

        # We consider slopes less that 15% to be neutral.
        # Set aspect to aspect_midpoint_neutral_east (ie, typically 90 degrees)
        data["aspect"][
            data["slope"] < config["aspect_neutral_slope_threshold_percent"]
        ] = config["aspect_midpoint_neutral_east_degrees"]

        # ----------------------------------------------------------------
        # convert rule polygons to raster and expand the outer rule bounds
        # ----------------------------------------------------------------
        # load to raster
        rules = features.rasterize(
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

        # create binary inverted rules image to calc distance from
        # zero values to a rule poly
        a = np.where(rules == 0, 1, 0)

        # compute the distance (euclidian distance in cell units)
        # and also return the index of the nearest rule poly
        # (allowing us to perform 'Euclidean Allocation')
        b, c = ndimage.distance_transform_edt(a, return_indices=True)

        # extract only the part of the feature transform within
        # our expansion distance
        expand_bounds_cells = ceil(
            (config["expand_bounds_metres"] / config["cell_size_metres"])
        )
        data["ruleimg"] = np.where(b < expand_bounds_cells, rules[c[0], c[1]], 0)

        self.data = data

    def model(self):
        """
        Generate initial becvalue raster.

        Create the raster by iterating through elevation table,
        setting output raster to becvalue for each row where criteria are met
        by the dem/aspect/rulepolys.
        """
        LOG.info("Generating initial becvalue raster:")
        # shortcut
        data = self.data

        # Create the initial bec model
        # We assign beclabels based on elevation / aspect / rule polygon.
        # Elevations in the source elevation table are stretched across
        # the aspect temperature zones (cool/neutral/warm/neutral/cool) in
        # an effort to smooth out transitions values between aspects
        data["becinit"] = np.zeros(shape=self.shape, dtype="uint16")
        # iterate through rows in elevation table
        elevation_rows = data["elevation"].to_dict("records")
        with click.progressbar(elevation_rows) as bar:
            for row in bar:
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
                            data["becinit"][
                                (data["ruleimg"] == row["polygon_number"])
                                & (data["aspect"] >= aspect_min)
                                & (data["dem"] >= elev_min)
                                & (data["dem"] < elev_max)
                            ] = self.becvalue_lookup[row["beclabel"]]
                            # now start at zero
                            aspect_min = 0

                        # assign becvalues based on rule & min/max elev/aspect
                        data["becinit"][
                            (data["ruleimg"] == row["polygon_number"])
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

        # before performing the majority filter, group high elevation
        # labels across rule polygons (alpine, parkland, woodland)
        data["becinit_grouped"] = data["becinit"].copy()

        # define new becvalues for aggregated high elevation labels
        # generate these dynamically based on current max value because using
        # arbitrary large values decreases performace of scikit-img rank
        # filters (majority)
        max_value = data["becmaster"]["becvalue"].max()
        high_elevation_aggregates = {
            "alpine": max_value + 1,
            "parkland": max_value + 2,
            "woodland": max_value + 3,
        }
        for key in high_elevation_aggregates:
            for becvalue in self.high_elevation_dissolves[key]:
                data["becinit_grouped"] = np.where(
                    data["becinit_grouped"] == becvalue,
                    high_elevation_aggregates[key],
                    data["becinit_grouped"],
                )

        # note original high elevation becvalues
        high_elevation_becvalues = (
            self.high_elevation_dissolves["alpine"]
            + self.high_elevation_dissolves["parkland"]
            + self.high_elevation_dissolves["woodland"]
        )

        # ----------------------------------------------------------------
        # majority filter
        # ----------------------------------------------------------------
        LOG.info("Running majority filter")
        data["majority"] = np.where(
            data["slope"] < config["majority_filter_steep_slope_threshold_percent"],
            majority(
                data["becinit_grouped"],
                morphology.rectangle(
                    width=self.filtersize_low, height=self.filtersize_low
                ),
            ),
            majority(
                data["becinit_grouped"],
                morphology.rectangle(
                    width=self.filtersize_steep, height=self.filtersize_steep
                ),
            ),
        )

        # to ungroup the high elevation values while retaining the result of
        # the majority filter, loop through the rule polygons and re-assign
        # the becvalues
        data["postmajority"] = data["majority"].copy()
        for zone in ["alpine", "parkland", "woodland"]:
            for lookup in [r for r in self.high_elevation_merges if r["type"] == zone]:
                data["postmajority"][
                    (data["ruleimg"] == lookup["rule"])
                    & (data["majority"] == high_elevation_aggregates[zone])
                ] = lookup["becvalue"]

        # ----------------------------------------------------------------
        # Basic noise filter
        # Remove holes < the noise_removal_threshold within each zone
        # ----------------------------------------------------------------
        LOG.info("Running noise removal filter")

        # convert noise_removal_threshold value from ha to n cells
        noise_threshold = int(
            (config["noise_removal_threshold_ha"] * 10000)
            / (config["cell_size_metres"] ** 2)
        )

        # initialize the output raster for noise filter
        data["noise"] = np.zeros(shape=self.shape, dtype="uint16")

        # process each non zero becvalues
        for becvalue in [v for v in self.beclabel_lookup if v != 0]:

            # extract given becvalue
            X = np.where(data["postmajority"] == becvalue, 1, 0)

            # fill holes, remove small objects
            Y = morphology.remove_small_holes(
                X, noise_threshold, connectivity=config["cell_connectivity"]
            )
            Z = morphology.remove_small_objects(
                Y, noise_threshold, connectivity=config["cell_connectivity"]
            )

            # insert values into output
            data["noise"] = np.where(Z != 0, becvalue, data["noise"])

        # ----------------------------------------------------------------
        # Fill holes introduced by noise filter
        #
        # The noise filter removes small holes / objects surrounded by
        # contiguous zones.
        # When a small area is bordered by more than 1 becvalue, it does not
        # get filled and leaves a hole.
        # Fill these holes using the distance transform (as done with
        # expansion of rule polys). Restrict the expansion to within the rule
        # polys only, otherwise the results bleed to the edges of the extent
        # (note that this removes need for area closing, edges are filled too)
        # ----------------------------------------------------------------
        a = np.where(data["noise"] == 0, 1, 0)
        b, c = ndimage.distance_transform_edt(a, return_indices=True)
        data["noise_fill"] = np.where(
            (data["noise"] == 0) & (data["ruleimg"] != 0),
            data["noise"][c[0], c[1]],
            data["noise"],
        )

        # ----------------------------------------------------------------
        # High elevation noise removal
        # Process alpine / parkland / woodland / high elevation labels
        # and merge the with the label below if not of sufficent size
        # ----------------------------------------------------------------
        # initialize output image
        data["highelev"] = data["noise_fill"].copy()
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
            LOG.info(
                "Running high_elevation_removal_threshold on {}".format(highelev_type)
            )

            # Extract area of interest
            # eg, find and aggregate all parkland values for finding alpine
            # area < threshold
            to_agg = self.high_elevation_dissolves[high_elevation_types[i + 1]]
            X = np.isin(data["highelev"], to_agg)
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
                data["highelev"] = np.where(
                    (Y == 1) & (data["ruleimg"] == merge["rule"]),
                    merge["becvalue_target"],
                    data["highelev"],
                )

        # ----------------------------------------------------------------
        # Convert to poly
        # ----------------------------------------------------------------
        fc = FeatureCollection(
            [
                Feature(geometry=s, properties={"becvalue": v})
                for i, (s, v) in enumerate(
                    shapes(
                        data["highelev"],
                        transform=self.transform,
                        connectivity=(config["cell_connectivity"] * 4),
                    )
                )
            ]
        )
        data["becvalue_polys"] = gpd.GeoDataFrame.from_features(fc)

        # add beclabel column to output polygons
        data["becvalue_polys"]["BGC_LABEL"] = data["becvalue_polys"]["becvalue"].map(
            self.beclabel_lookup
        )

        # set crs
        data["becvalue_polys"].crs = {"init": "epsg:3005"}

        # add area_ha column
        data["becvalue_polys"]["AREA_HECTARES"] = (
            data["becvalue_polys"]["geometry"].area / 10000
        )

        # round to 1 decimal place
        data["becvalue_polys"].AREA_HECTARES = data[
            "becvalue_polys"
        ].AREA_HECTARES.round(1)

        # clip to aggregated rule polygons
        # (buffer the dissolved rules out and in to ensure no small holes
        # are created by dissolve due to precision errors)
        data["rulepolys"]["rules"] = 1
        X = data["rulepolys"].dissolve(by="rules").buffer(.01).buffer(-.01)
        Y = gpd.GeoDataFrame(X).rename(columns={0: "geometry"}).set_geometry("geometry")
        data["becvalue_polys"] = gpd.overlay(
            data["becvalue_polys"], Y, how="intersection"
        )

        # remove rulepoly fields
        data["becvalue_polys"] = data["becvalue_polys"][
            ["BGC_LABEL", "AREA_HECTARES", "becvalue", "geometry"]
        ]

        self.data = data

    def write(self, discard_temp=False):
        """ Write outputs to disk
        """

        # if not specified otherwise, dump all intermediate raster data to file
        if not discard_temp:
            # loop through everything loaded to the .data dictionary
            # and write/index if it is a numpy array
            qa_dumps = [d for d in self.data.keys() if type(self.data[d]) == np.ndarray]
            # read DEM to get crs / width / height etc
            with rasterio.open(
                os.path.join(self.config["wksp"], "src", "dem.tif")
            ) as src:
                for i, raster in enumerate(qa_dumps):
                    out_qa_tif = os.path.join(
                        self.config["wksp"], str(i).zfill(2) + "_" + raster + ".tif"
                    )
                    with rasterio.open(
                        out_qa_tif,
                        "w",
                        driver="GTiff",
                        dtype=rasterio.int16,
                        count=1,
                        width=src.width,
                        height=src.height,
                        crs=src.crs,
                        transform=src.transform,
                        nodata=src.nodata,
                    ) as dst:
                        dst.write(self.data[raster].astype(np.int16), indexes=1)

            # remind user where to find QA data
            LOG.info("QA files are here: {}".format(self.config["temp_folder"]))

        # remove becvalue column
        self.data["becvalue_polys"] = self.data["becvalue_polys"].drop(
            columns=["becvalue"]
        )

        # write output vectors to file
        self.data["becvalue_polys"].to_file(
            self.config["out_file"], layer=self.config["out_layer"], driver="GPKG"
        )

        # dump config settings to file
        self.write_config_log()

        LOG.info("Output {} created".format(self.config["out_file"]))
