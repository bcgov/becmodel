import os
import becmodel
from becmodel.config import config
from becmodel import util

import pytest


def test_invalid_config():
    with pytest.raises(ValueError):
        util.load_config("tests/test_invalid_config.cfg")


def test_valid_config():
    util.load_config("tests/test_config.cfg")
    assert config["rulepolygon_file"] == "tests/data/data.gdb.zip"


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
