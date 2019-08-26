import click

from becmodel import BECModel


@click.command()
@click.option("-v", "--validate", is_flag=True, help="Validate inputs - do not run model")
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing outputs")
@click.option("-qa", "--qa", is_flag=True, help="Write temp files to disk for QA")
@click.argument("config_file", type=click.Path(exists=True), required=False)
def cli(config_file, overwrite, qa, validate):
    BM = BECModel(config_file)
    if validate:
        click.echo("becmodel: data validation successful")
    else:
        BM.load(overwrite=overwrite)
        BM.model()
        BM.postfilter()
        BM.write(qa)
