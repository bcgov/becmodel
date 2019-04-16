import os
import becmodel
from becmodel.config import config
from becmodel import util
from becmodel.util import ConfigError, ConfigValueError
import pytest


def test_invalid_config():
    with pytest.raises(ConfigError):
        util.load_config("tests/test_invalid_config.cfg")


def test_valid_config():
    util.load_config("tests/test_config.cfg")
    assert config["rulepolygon_file"] == "tests/data/data.gdb.zip"


def test_config_data_missing():
    with pytest.raises(ConfigValueError):
        util.load_config("tests/test_config.cfg")
        config["rulepolygon_file"] = "nodata.gdb"
        util.validate_config()


def test_invalid_rule_layer():
    with pytest.raises(ConfigValueError):
        util.load_config("tests/test_config.cfg")
        config["rulepolygon_layer"] = "nodata"
        util.validate_config()


def test_load(tmpdir):
    """ Check that data load is successful
    """
    util.load_config("tests/test_config.cfg")
    config["wksp"] = str(tmpdir)
    becmodel.load()
    assert os.path.exists(tmpdir.join("dem.tif"))
    assert os.path.exists(tmpdir.join("aspect.tif"))
    assert os.path.exists(tmpdir.join("aspect_class.tif"))
    assert os.path.exists(tmpdir.join("rules.tif"))
