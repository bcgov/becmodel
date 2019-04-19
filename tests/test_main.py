import os

import pytest
import pandas as pd

import becmodel
from becmodel.config import config
from becmodel import util
from becmodel.util import ConfigError, ConfigValueError, DataValueError


def test_invalid_config():
    with pytest.raises(ConfigError):
        util.load_config("tests/test_invalid_config.cfg")


def test_valid_config():
    util.load_config("tests/test.cfg")
    assert config["rulepolys_file"] == "tests/data/data.gdb.zip"


def test_config_data_missing():
    with pytest.raises(ConfigValueError):
        util.load_config("tests/test.cfg")
        config["rulepolys_file"] = "nodata.gdb"
        util.validate_config()


def test_invalid_rule_layer():
    with pytest.raises(ConfigValueError):
        util.load_config("tests/test.cfg")
        config["rulepolys_layer"] = "nodata"
        util.validate_config()


def test_load_tables():
    util.load_config("tests/test.cfg")
    data = util.load_tables()
    assert data["elevation"].becvalue[0] == 265


# invalid types in rule polys
def test_load_invalid_rulepolys():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["rulepolys_file"] = "tests/data/invalid_data.gdb.zip"
        util.load_tables()


# invalid types in becmaster
def test_load_invalid_becmaster():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["becmaster"] = "tests/data/becmaster_invalid.csv"
        util.load_tables()


# elevation and rulepolys polygon_number values are not exact matches
def test_load_invalid_elevation():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["elevation"] = "tests/data/elevation_invalid.csv"
        util.load_tables()


def test_load_invalid_elevation_bands():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        data = util.load_tables()
        bad_elevation = {
            "polygon_number": [1, 1, 1, 1],
            "cool_low": [0, 530, 875, 1400],
            "cool_high": [531, 875, 1400, 10000],
            "neutral_low": [0, 525, 875, 1400],
            "neutral_high": [525, 875, 1400, 10000],
            "warm_low": [0, 525, 875, 1400],
            "warm_high": [525, 875, 1400, 10000]
        }
        data["elevation"] = pd.DataFrame(bad_elevation)
        data["rulepolys"] = pd.DataFrame({"polygon_number": [1]})
        util.validate_data(data)


def test_process(tmpdir):
    """ Check that data load is successful
    """
    util.load_config("tests/test.cfg")
    config["wksp"] = str(tmpdir)
    becmodel.process()
    assert os.path.exists(tmpdir.join("dem.tif"))
    assert os.path.exists(tmpdir.join("aspect.tif"))
    assert os.path.exists(tmpdir.join("aspect_class.tif"))
    assert os.path.exists(tmpdir.join("rules.tif"))
    assert os.path.exists(tmpdir.join("becvalue.tif"))
