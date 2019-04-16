import os
import configparser
import logging
import logging.handlers

from becmodel.config import config


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
            raise ValueError("Config key {} is invalid".format(key))
        config[key] = cfg_dict[key]


def validate_config():
    """Make sure specified paths/files exist
    """
    # validate that files exist
    for key in ["rulepolygon_file", "rulepolygon_table", "elevation", "becmaster"]:
        if not os.path.exists(config[key]):
            raise ValueError("config {}: {} does not exist".format(key, config[key]))

    # check that rule poly layer exists
    #"rulepolygon_layer": "rule_polys"

    # check that integer keys are integers

    for key in ["cell_size","smoothing_tolerance","generalize_tolerance","parkland_removeal_threshold","noise_removal_threshold","expand_bounds"]:
        if not type(key) is int:
            raise ValueError("config {}: {} is invalid, it must be an integer".format(key, config[key]))


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
