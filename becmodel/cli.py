import click

from becmodel import BECModel


@click.command()
@click.option("-v", "--validate", is_flag=True)
@click.option("-o", "--overwrite", is_flag=True)
@click.option("-qa", "--qa", is_flag=True)
@click.argument("config_file", type=click.Path(exists=True))
def cli(config_file, overwrite, qa, validate):
    BM = BECModel(config_file)
    if validate:
        BM.validate()
        click.echo("becmodel: data validation successful")
    else:
        BM.load(overwrite=overwrite)
        BM.model()
        BM.postfilter()
        BM.write(qa)
