import os
from pathlib import Path
import logging
import tempfile
import subprocess

import mercantile
import rasterio
from rasterio.merge import merge as riomerge
import requests


BASE_URL = "http://s3.amazonaws.com/elevation-tiles-prod/geotiff/"
LOG = logging.getLogger(__name__)


def tile_path(tile):
    return list(map(str, [tile.z, tile.x, str(tile.y) + ".tif"]))


def download_tile(tile, out_path, verbose=True):
    """
    Download a terrain tile to out_path, where tile is a Mercantile Tile
    """
    out_file = Path(out_path).joinpath("/".join(tile_path(tile)))
    out_file.parent.mkdir(parents=True, exist_ok=True)
    url = BASE_URL + "/".join(tile_path(tile))
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise RuntimeError("No such tile: {}".format("/".join(tile_path(tile))))
    LOG.debug(f"Downloaded {url}")
    with out_file.open("wb") as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)


def download(bounds, zoom, cache):
    """
    Download geotiffs that intersect provided bounds
    (if not already present in cache)
    Modified from https://github.com/interline-io/planetutils
    """
    tiles = mercantile.tiles(*bounds, zoom)
    found = set()
    to_download = set()

    for tile in tiles:
        if Path(cache).joinpath("/".join(tile_path(tile))).exists():
            found.add(tile)
        else:
            to_download.add(tile)
    LOG.info("Downloading %s tiles" % (len(to_download)))
    for tile in sorted(to_download):
        download_tile(tile, cache)


def merge(bounds, zoom, cache, dst_crs="EPSG:3005", res=30, out_file=None):
    """
    Given bounds and zoom,
        - get terrain tiles
        - merge
        - dump to int16 geotiff using specified crs and resolution
    """
    # make sure tiles are available
    download(bounds, zoom, cache)

    # check that provided tiles are at a single zoom
    n_zooms = len(set([tile.z for tile in mercantile.tiles(*bounds, zoom)]))
    if n_zooms > 1:
        raise ValueError(
            f"{n_zooms} zooms given - provide tiles at a single zoom level"
        )

    found = []
    for tile in mercantile.tiles(*bounds, zoom):
        tif = Path(cache).joinpath("/".join(tile_path(tile)))
        if tif.exists():
            found.append(rasterio.open(str(tif)))
        else:
            raise RuntimeError("{tif} does not exist")

    if len(found) == 0:
        raise RuntimeError("No files present")

    mosaic, out_trans = riomerge(found)
    out_meta = found[0].meta.copy()
    out_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "crs": "EPSG:3785",
        }
    )

    # write temporary merged tiff
    temp_tif = os.path.join(tempfile.mkdtemp(prefix="collected-"), "temp.tif")
    with rasterio.open(temp_tif, "w", **out_meta) as dest:
        dest.write(mosaic)
    if not out_file:
        out_file = os.path.join(tempfile.mkdtemp(prefix="warped-"), "out.tif")

    # reproject, resample, convert to int16, -9999 as nodata
    cmd = [
        "gdalwarp",
        "-t_srs",
        dst_crs,
        "-r",
        "cubic",
        "-ot",
        "Int16",
        "-dstnodata",
        "-9999",
        temp_tif,
        out_file,
    ]
    if res:
        cmd.extend(["-tr", str(res), str(res), "-tap"])
    subprocess.run(cmd)

    return out_file
