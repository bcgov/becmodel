import os
import becmodel
from becmodel.config import config
from becmodel import util
from becmodel.util import ConfigError, ConfigValueError, DataValueError
import pytest


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


def test_load_data():
    util.load_config("tests/test.cfg")
    data = util.load_data()
    assert data["elevation"].becvalue[0] == 265


def test_load_invalid_rulepolys():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["rulepolys_file"] = "tests/data/invalid_data.gdb.zip"
        util.load_data()


def test_load_invalid_becmaster():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["becmaster"] = "tests/data/becmaster_invalid.csv"
        util.load_data()


def test_load_invalid_elevation():
    with pytest.raises(DataValueError):
        util.load_config("tests/test.cfg")
        config["elevation"] = "tests/data/elevation_invalid.csv"
        util.load_data()


def test_load(tmpdir):
    """ Check that data load is successful
    """
    util.load_config("tests/test.cfg")
    config["wksp"] = str(tmpdir)
    becmodel.load()
    assert os.path.exists(tmpdir.join("dem.tif"))
    assert os.path.exists(tmpdir.join("aspect.tif"))
    assert os.path.exists(tmpdir.join("aspect_class.tif"))
    assert os.path.exists(tmpdir.join("rules.tif"))
