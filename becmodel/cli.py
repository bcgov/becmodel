import logging

import click

import becmodel
from becmodel import util


util.configure_logging()
log = logging.getLogger(__name__)


@click.command()
@click.option("-v", "--validate", is_flag=True)
@click.option("-o", "--overwrite", is_flag=True)
@click.argument("config_file", type=click.Path(exists=True))
def cli(config_file, overwrite, validate):
    log.info("Initializing BEC model v{}".format(becmodel.__version__))
    log.info("Loading config from file: %s", config_file)
    util.load_config(config_file)
    if validate:
        util.load_tables()
    else:
        becmodel.process(overwrite)
