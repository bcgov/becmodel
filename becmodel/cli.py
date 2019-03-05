import os
import logging

import click

import becmodel
from becmodel import util
from becmodel.config import config


util.configure_logging()
log = logging.getLogger(__name__)


@click.command()
@click.option("--config_file", default="bec_config.cfg")
def cli(config_file):
    # update config if a config file provided
    if os.path.exists(config_file):
        log.info("Loading config from file: %s", config_file)
        util.load_config(config_file)
    else:
        log.info("Using default configuration")
    log.info("Running the bec model")
    # load data
    becmodel.load()
