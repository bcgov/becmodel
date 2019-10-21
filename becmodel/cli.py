import click
import logging
import sys

from cligj import verbose_opt, quiet_opt

from becmodel import BECModel


@click.command()
@click.option(
    "-v", "--validate", is_flag=True, help="Validate inputs - do not run model"
)
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing outputs")
@click.option("-qa", "--qa", is_flag=True, help="Write temp files to disk for QA")
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def cli(config_file, overwrite, qa, validate, verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(stream=sys.stderr, level=log_level)

    BM = BECModel(config_file)
    if validate:
        click.echo("becmodel: data validation successful")
    else:
        BM.load(overwrite=overwrite)
        BM.model()
        BM.postfilter()
        BM.write(qa)
