import os
import configparser
import logging
import logging.handlers

import fiona

from becmodel.config import config


class ConfigError(Exception):
    """Configuration key error"""


class ConfigValueError(Exception):
    """Configuration key error"""


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
    for key in ["rulepolygon_file", "elevation", "becmaster"]:
        if not os.path.exists(config[key]):
            raise ConfigValueError("config {}: {} does not exist".format(key, config[key]))

    # validate rule polygon layer exists
    if config["rulepolygon_layer"] not in fiona.listlayers(config["rulepolygon_file"]):
        raise ConfigValueError("config {}: {} does not exist in {}".format(key, config["rulepolygon_layer"], config["rulepolygon_file"]))

    # todo - perhaps validate various int param are within reasonable range?


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
