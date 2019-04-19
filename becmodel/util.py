import os
import configparser
import logging
import logging.handlers
import csv

import pandas as pd
import numpy as np
import geopandas as gpd
import fiona

from becmodel.config import config

# input data tables must match this structure
ELEVATION = {
    "becvalue": np.int32,
    "beclabel": np.str,
    "class_name": np.str,
    "cool_low": np.int32,
    "cool_high": np.int32,
    "neutral_low": np.int32,
    "neutral_high": np.int32,
    "warm_low": np.int32,
    "warm_high": np.int32,
    "polygon_number": np.int32
}

RULEPOLYS = {
  "polygon_number": np.int32,
  "polygon_description": np.str
}

BECMASTER = {
    "becvalue": np.int32,
    "beclabel": np.str,
    "zone": np.str,
    "subzone": np.str,
    "variant": np.str,
    "phase": np.str
}


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
    for key in ["cell_size","smoothing_tolerance","generalize_tolerance","parkland_removeal_threshold","noise_removal_threshold","expand_bounds"]:
        config[key] = int(config[key])

    validate_config()


def validate_config():
    # validate that required paths exist
    for key in ["rulepolys_file", "elevation", "becmaster"]:
        if not os.path.exists(config[key]):
            raise ConfigValueError("config {}: {} does not exist".format(key, config[key]))

    # validate rule polygon layer exists
    if config["rulepolys_layer"] not in fiona.listlayers(config["rulepolys_file"]):
        raise ConfigValueError("config {}: {} does not exist in {}".format(key, config["rulepolys_layer"], config["rulepolys_file"]))


def load_data():
    """load data from files specified in config
    """
    data = {}
    try:
        data["elevation"] = pd.read_csv(config["elevation"], dtype=ELEVATION)
        data["becmaster"] = pd.read_csv(config["becmaster"], dtype=BECMASTER)
        data["rulepolys"] = gpd.read_file(config["rulepolys_file"], layer=config["rulepolys_layer"])
        data["rulepolys"] = data["rulepolys"].astype(RULEPOLYS, errors='raise')
    except:
        raise DataValueError("Value(s) in input files incorrect, check data types")

    # lowercaseify the column names
    for df in data.values():
        df.columns = df.columns.str.lower()

    # do polygon numbers match in each table?
    rules = set(data["rulepolys"].polygon_number.unique())
    elev = set(data["elevation"].polygon_number.unique())
    if rules ^ elev:
        raise DataValueError("polygon_number values are not equivalent in input rulepolys and elevation")

    return data


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
