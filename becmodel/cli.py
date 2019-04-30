import logging

import click

import becmodel
from becmodel import util


util.configure_logging()
log = logging.getLogger(__name__)


@click.command()
@click.option("-v", "--validate", is_flag=True)
@click.option("-o", "--overwrite", is_flag=True)
@click.option("-qa", "--qa", is_flag=True)
@click.argument("config_file", type=click.Path(exists=True))
def cli(config_file, overwrite, qa, validate):
    log.info("Initializing BEC model v{}".format(becmodel.__version__))

    if validate:
        becmodel.validate(config_file=config_file)
    else:
        data = becmodel.load(config_file=config_file, overwrite=overwrite)
        becmodel.write(data, qa)
