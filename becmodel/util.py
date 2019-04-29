from math import trunc
import os
import configparser
import logging
import logging.handlers

import pandas as pd
import numpy as np
import geopandas as gpd
import fiona

from becmodel.config import config


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration value error"""


class DataValueError(Exception):
    """error in input dataset"""


def make_sure_path_exists(path):
    """Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    :param path: string
    """
    try:
        os.makedirs(path)
    except:
        pass


def align(bounds):
    """Adjust input bounds to align with Hectares BC raster
    (round bounds to nearest km, then shift by 12.5m)
    """
    ll = [((trunc(b / 100) * 100) - 12.5) for b in bounds[:2]]
    ur = [(((trunc(b / 100) + 1) * 100) + 87.5) for b in bounds[2:]]
    return (ll[0], ll[1], ur[0], ur[1])


def load_config(config_file):
    """Read provided config file, overwriting default config values
    """
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    cfg_dict = dict(cfg["CONFIG"])

    for key in cfg_dict:
        if key not in config.keys():
            raise ConfigError("Config key {} is invalid".format(key))
        config[key] = cfg_dict[key]

    # convert int config values to int
    for key in [
        "output_cell_size",
        "dem_cell_size",
        "smoothing_tolerance",
        "generalize_tolerance",
        "parkland_removal_threshold",
        "noise_removal_threshold",
        "expand_bounds",
    ]:
        config[key] = int(config[key])

    validate_config()


def validate_config():
    # validate that required paths exist
    for key in ["rulepolys_file", "elevation", "becmaster"]:
        if not os.path.exists(config[key]):
            raise ConfigValueError(
                "config {}: {} does not exist".format(key, config[key])
            )

    # validate rule polygon layer exists
    if config["rulepolys_layer"] not in fiona.listlayers(config["rulepolys_file"]):
        raise ConfigValueError(
            "config {}: {} does not exist in {}".format(
                key, config["rulepolys_layer"], config["rulepolys_file"]
            )
        )


def load_tables():
    """load data from files specified in config and validate
    """

    # to support useing existing input files, remap the short dbase compatible
    # column names to standard
    elevation_column_remap = {
        "classnm": "class_name",
        "neut_low": "neutral_low",
        "neut_high": "neutral_high",
        "polygonnbr": "polygon_number"
    }
    rules_column_remap = {
        "polygonnbr": "polygon_number",
        "polygondes": "polygon_description"
    }

    data = {}
    try:
        # -- elevation
        data["elevation"] = pd.read_csv(config["elevation"])
        data["elevation"].rename(columns=str.lower, inplace=True)
        data["elevation"].rename(columns=elevation_column_remap, inplace=True)
        data["elevation"].astype(
            {
                "becvalue": np.int16,
                "beclabel": np.str,
                "class_name": np.str,
                "cool_low": np.int16,
                "cool_high": np.int16,
                "neutral_low": np.int16,
                "neutral_high": np.int16,
                "warm_low": np.int16,
                "warm_high": np.int16,
                "polygon_number": np.int16,
            },
        )

        # -- becmaster
        data["becmaster"] = pd.read_csv(
            config["becmaster"],
            dtype={
                "becvalue": np.int16,
                "beclabel": np.str,
                "zone": np.str,
                "subzone": np.str,
                "variant": np.str,
                "phase": np.str,
            },
        )
        data["becmaster"].rename(columns=str.lower, inplace=True)

        # -- rule polys
        data["rulepolys"] = gpd.read_file(
            config["rulepolys_file"], layer=config["rulepolys_layer"]
        )
        data["rulepolys"].rename(columns=str.lower, inplace=True)
        data["rulepolys"].rename(columns=rules_column_remap, inplace=True)
        data["rulepolys"] = data["rulepolys"].astype(
            {"polygon_number": np.int16, "polygon_description": np.str},
            errors="raise"
        )
    except:
        raise DataValueError(
            "Column names or value(s) in input files incorrect. "
            "Check column names and data types"
        )

    validate_data(data)

    return data


def validate_data(data):
    """apply some simple checks to make sure inputs make sense
    """

    # do polygon numbers match in each table?
    rulepolynums = set(data["rulepolys"].polygon_number.unique())
    elevpolynums = set(data["elevation"].polygon_number.unique())
    if rulepolynums ^ elevpolynums:
        raise DataValueError(
            "input file polygon_number values do not match: \n  rulepolys: {} \n  elevation: {}".format(
                str(rulepolynums - elevpolynums), str(elevpolynums - rulepolynums)
            )
        )

    # check that elevation table values are continuous
    for poly in data["elevation"].polygon_number.unique():
        for temp in ["cool", "neutral", "warm"]:
            # get the elevation ranges (low, high) values for the temp
            elev_values = sorted(
                list(
                    data["elevation"][data["elevation"].polygon_number == poly][
                        temp + "_low"
                    ]
                )
                + list(
                    data["elevation"][data["elevation"].polygon_number == poly][
                        temp + "_high"
                    ]
                )
            )
            # strip off the max and min
            elev_values = elev_values[1:-1]
            # there must be an even number of elevations provided
            if len(elev_values) % 2 != 0:
                raise DataValueError(
                    "Elevations are poorly structured, see {} columns for polygon_number {}".format(
                        temp, poly
                    )
                )
            # elevations must also be consecutive, no gaps in values
            # when low/high columns are combined and values are sorted, the
            # values are always like this:
            #  [100, 100, 500, 500, 1000, 1000]
            # Therefore, length of the list / 2 is always equal to length of
            # the set of unique values.
            if len(elev_values) / 2 != len(set(elev_values)):
                raise DataValueError(
                    "Elevations are poorly structured, see {} columns for polygon_number {}".format(
                        temp, poly
                    )
                )


def configure_logging():
    logger = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    logger.setLevel(logging.INFO)

    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(formatter)
    streamhandler.setLevel(logging.INFO)
    logger.addHandler(streamhandler)

    filehandler = logging.handlers.TimedRotatingFileHandler(
        config["log_file"], when="D", interval=7, backupCount=10
    )
    filehandler.setFormatter(formatter)
    filehandler.setLevel(logging.INFO)
    logger.addHandler(filehandler)
