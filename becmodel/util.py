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
        config[key] = cfg_dict[key]


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
