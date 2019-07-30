import os

import pytest
import pandas as pd

from becmodel import BECModel
from becmodel import util
from becmodel.main import ConfigError, ConfigValueError
from becmodel.util import DataValueError

TESTCONFIG = "tests/test.cfg"


def test_align():
    bounds = [1445933.56, 467399.57, 1463229.87, 488903.09]
    assert util.align(bounds) == (1445887.5, 467287.5, 1463387.5, 489087.5)


def test_invalid_config():
    with pytest.raises(ConfigError):
        BM = BECModel("tests/test_invalid_config.cfg")


def test_valid_config():
    BM = BECModel(TESTCONFIG)
    assert BM.config["rulepolys_file"] == "tests/data/data.gdb.zip"


def test_config_data_missing():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"rulepolys_file": "nodata.gdb"})


def test_invalid_rule_layer():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"rulepolys_layer": "nodata"})


def test_reproject_rule_layer():
    BM = BECModel(TESTCONFIG)
    BM.update_config({"rulepolys_file": "tests/data/rulepolys.geojson"})
    BM.update_config({"rulepolys_layer": None})
    BM.load()
    assert BM.data["rulepolys"].crs == {"init": "EPSG:3005"}


def test_invalid_cell_size1():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"cell_size_metres": 110})


def test_invalid_cell_size2():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"cell_size_metres": 20})


def test_invalid_cell_size3():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"cell_size_metres": 26})


def test_load_tables():
    BM = BECModel(TESTCONFIG)
    assert BM.data["elevation"].beclabel[0] == "MS  xk 1"


def test_load_excel():
    BM = BECModel(TESTCONFIG)
    BM.update_config({"elevation": "tests/data/elevation.xlsx"})
    assert BM.data["elevation"].beclabel[0] == "MS  xk 1"


def test_prefilter(tmpdir):
    BM = BECModel("tests/test.cfg")
    BM.update_config({"temp_folder": str(tmpdir)})
    BM.update_config({"dem_prefilter": "True"})
    BM.load()
    assert os.path.exists(tmpdir.join("dem_filtered.tif"))


# invalid types in rule polys
def test_load_invalid_rulepolys():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"rulepolys_file": "tests/data/invalid_data.gdb.zip"}, reload=True)


# elevation and rulepolys polygon_number values are not exact matches
def test_load_invalid_elevation():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"elevation": "tests/data/elevation_invalid.csv"}, reload=True)


def test_load_invalid_elevation_bands():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        bad_elevation = {
            "polygon_number": [1, 1, 1, 1],
            "cool_low": [0, 530, 875, 1400],
            "cool_high": [531, 875, 1400, 10000],
            "neutral_low": [0, 525, 875, 1400],
            "neutral_high": [525, 875, 1400, 10000],
            "warm_low": [0, 525, 875, 1400],
            "warm_high": [525, 875, 1400, 10000]
        }
        BM.data["elevation"] = pd.DataFrame(bad_elevation)
        BM.data["rulepolys"] = pd.DataFrame({"polygon_number": [1]})
        util.validate_data(BM.data)


def test_run(tmpdir):
    """ Check that outputs are created, not necessarily correct...
    """
    BM = BECModel("tests/test.cfg")
    BM.update_config({"temp_folder": str(tmpdir)})
    BM.load()
    BM.model()
    BM.postfilter()
    BM.write()
    assert os.path.exists(tmpdir.join("dem.tif"))
    assert os.path.exists(tmpdir.join("aspect.tif"))
    assert os.path.exists(tmpdir.join("becmodel.gpkg"))
