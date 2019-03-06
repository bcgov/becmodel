import logging

import click

import becmodel
from becmodel import util
from becmodel.config import config


util.configure_logging()
log = logging.getLogger(__name__)


@click.command()
@click.option("-v", "--validate", is_flag=True)
@click.argument("config_file", type=click.Path(exists=True))
def cli(config_file, validate):
    log.info("Loading config from file: %s", config_file)
    util.load_config(config_file)
    log.info("Running the bec model")
    # validate the provided inputs
    #becmodel.validate()
    # load data
    if not validate:
        becmodel.load()
