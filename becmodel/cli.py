import click
import logging
import sys

from cligj import verbose_opt, quiet_opt

from becmodel import BECModel


@click.command()
@click.option(
    "-dr", "--dry_run", "--dry-run", is_flag=True, help="Validate inputs - do not run model"
)
@click.option(
    "-l", "--load", is_flag=True, help="Download input datasets - do not run model"
)
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing outputs")
@click.option("-d", "--discard-temp", "--discard_temp", is_flag=True, help="Do not write temp files to disk")
@click.argument("config_file", type=click.Path(exists=True), required=False)
@verbose_opt
@quiet_opt
def cli(config_file, overwrite, discard_temp, dry_run, load, verbose, quiet):
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)  # default to INFO log level
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    )

    BM = BECModel(config_file)
    if dry_run:
        click.echo("becmodel: Basic input data validation successful")
    elif load:
        BM.load(overwrite=overwrite)
    else:
        BM.load(overwrite=overwrite)
        BM.model()
        BM.postfilter()
        BM.write(discard_temp)
