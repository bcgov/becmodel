
from math import trunc
import os
from pathlib import Path

import logging
import logging.handlers

import pandas as pd
import numpy as np
import geopandas as gpd


log = logging.getLogger(__name__)


class DataValueError(Exception):
    """error in input dataset"""


def make_sure_path_exists(path):
    """Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    :param path: string
    """
    try:
        os.makedirs(path)
    except:
        pass


def align(bounds):
    """
    Adjust input bounds to align with Hectares BC raster
    (round bounds to nearest 100m, then shift by 12.5m)
    """
    ll = [((trunc(b / 100) * 100) - 12.5) for b in bounds[:2]]
    ur = [(((trunc(b / 100) + 1) * 100) + 87.5) for b in bounds[2:]]
    return (ll[0], ll[1], ur[0], ur[1])


def load_tables(config):
    """load data from files specified in config and validate
    """

    # to support useing existing input files, remap the short dbase compatible
    # column names to standard
    elevation_column_remap = {
        "classnm": "class_name",
        "neut_low": "neutral_low",
        "neut_high": "neutral_high",
        "polygonnbr": "polygon_number",
    }
    rules_column_remap = {"polygonnbr": "polygon_number"}

    data = {}
    try:
        # -- elevation
        if Path(config["elevation"]).suffix == ".csv":
            data["elevation"] = pd.read_csv(config["elevation"])
        # if loading elevation table from Excel,
        # **values must be in the first worksheet**
        elif Path(config["elevation"]).suffix in [".xls", ".xlsx"]:
            data["elevation"] = pd.read_excel(config["elevation"], index_col=None)
        data["elevation"].rename(columns=str.lower, inplace=True)
        data["elevation"].rename(columns=elevation_column_remap, inplace=True)
        data["elevation"].astype(
            {
                "beclabel": np.str,
                "cool_low": np.int16,
                "cool_high": np.int16,
                "neutral_low": np.int16,
                "neutral_high": np.int16,
                "warm_low": np.int16,
                "warm_high": np.int16,
                "polygon_number": np.int16,
            }
        )
        # -- rule polys
        data["rulepolys"] = gpd.read_file(
            config["rulepolys_file"], layer=config["rulepolys_layer"]
        )
        # -- reproject if necessary
        if not data["rulepolys"].crs:
            raise DataValueError(
                "Input rule polygon projection undefined, define the projection in the input file before running becmodel"
            )

        elif data["rulepolys"].crs and data["rulepolys"].crs["init"].upper() !=  "EPSG:3005":
            log.info(
                "Input data is not specified as BC Albers, attempting to reproject"
            )
            data["rulepolys"] = data["rulepolys"].to_crs({"init": "EPSG:3005"})
        data["rulepolys"].rename(columns=str.lower, inplace=True)
        data["rulepolys"].rename(columns=rules_column_remap, inplace=True)
    except:
        raise DataValueError(
            "Column names or value(s) in input files incorrect. "
            "Check column names and data types"
        )

    validate_data(data)

    return data


def validate_data(data):
    """apply some simple checks to make sure inputs make sense
    """

    # do polygon numbers match in each table?
    rulepolynums = set(data["rulepolys"].polygon_number.unique())
    elevpolynums = set(data["elevation"].polygon_number.unique())
    if rulepolynums ^ elevpolynums:
        raise DataValueError(
            "input file polygon_number values do not match: \n  rulepolys: {} \n  elevation: {}".format(
                str(rulepolynums - elevpolynums), str(elevpolynums - rulepolynums)
            )
        )

    # check that elevation table values are continuous
    for poly in data["elevation"].polygon_number.unique():
        for temp in ["cool", "neutral", "warm"]:
            # get the elevation ranges (low, high) values for the temp
            elev_values = sorted(
                list(
                    data["elevation"][data["elevation"].polygon_number == poly][
                        temp + "_low"
                    ]
                )
                + list(
                    data["elevation"][data["elevation"].polygon_number == poly][
                        temp + "_high"
                    ]
                )
            )
            # strip off the max and min
            elev_values = elev_values[1:-1]
            # there must be an even number of elevations provided
            if len(elev_values) % 2 != 0:
                raise DataValueError(
                    "Elevations are poorly structured, see {} columns for polygon_number {}".format(
                        temp, poly
                    )
                )
            # elevations must also be consecutive, no gaps in values
            # when low/high columns are combined and values are sorted, the
            # values are always like this:
            #  [100, 100, 500, 500, 1000, 1000]
            # Therefore, length of the list / 2 is always equal to length of
            # the set of unique values.
            if len(elev_values) / 2 != len(set(elev_values)):
                raise DataValueError(
                    "Elevations are poorly structured, see {} columns for polygon_number {}".format(
                        temp, poly
                    )
                )


def configure_logging(config):
    logger = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    logger.setLevel(logging.INFO)

    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(formatter)
    streamhandler.setLevel(logging.INFO)
    logger.addHandler(streamhandler)


def multi2single(gdf):
    """
    multi to single is not a geopandas builtin
    https://github.com/geopandas/geopandas/issues/369
    """
    gdf_singlepoly = gdf[gdf.geometry.type == "Polygon"]
    gdf_multipoly = gdf[gdf.geometry.type == "MultiPolygon"]

    for i, row in gdf_multipoly.iterrows():
        Series_geometries = pd.Series(row.geometry)
        df = pd.concat(
            [gpd.GeoDataFrame(row, crs=gdf_multipoly.crs).T] * len(Series_geometries),
            ignore_index=True,
        )
        df["geometry"] = Series_geometries
        gdf_singlepoly = pd.concat([gdf_singlepoly, df])

    gdf_singlepoly.reset_index(inplace=True, drop=True)
    return gdf_singlepoly
