import os

import pytest
import pandas as pd
import fiona
import geopandas as gpd

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
    BM.update_config(
        {
            "rulepolys_file": "tests/data/rulepolys_4326.geojson",
            "rulepolys_layer": None
        },
        reload=True
    )
    assert BM.data["rulepolys"].crs["init"].upper() == "EPSG:3005"


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


def test_load_elevation():
    BM = BECModel(TESTCONFIG)
    assert BM.data["elevation"].beclabel[0] == "BG  xh 1"


def test_load_becmaster():
    BM = BECModel(TESTCONFIG)
    BM.update_config({"becmaster": "tests/data/becmaster_test.csv"}, reload=True)
    assert BM.data["becmaster"].becvalue[0] == 4


def test_load_becmaster_invalid_columns():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"becmaster": "tests/data/becmaster_invalid_cols.csv"}, reload=True)


def test_load_becmaster_invalid_data():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"becmaster": "tests/data/becmaster_invalid_data.csv"}, reload=True)


def test_load_excel():
    BM = BECModel(TESTCONFIG)
    BM.update_config({"elevation": "tests/data/elevation.xlsx"})
    assert BM.data["elevation"].beclabel[0] == "BG  xh 1"


# invalid types in rule polys
def test_load_invalid_rulepolys():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config(
            {"rulepolys_file": "tests/data/invalid_data.gdb.zip"}, reload=True
        )


# elevation and rulepolys polygon_number values are not exact matches
def test_load_invalid_elevation():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"elevation": "tests/data/elevation_invalid.csv"}, reload=True)


# elevation and rulepolys polygon_number values are not exact matches
def test_load_invalid_beclabel():
    with pytest.raises(DataValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"elevation": "tests/data/elevation_invalid_beclabel.csv"}, reload=True)


# test loading terrain tiles is successful
def test_load_terraintile_elevation(tmpdir):
    BM = BECModel(TESTCONFIG)

    BM.update_config(
        {
            "temp_folder": str(tmpdir),
            "rulepolys_file": "tests/data/rulepolys_4326_usa.geojson",
            "rulepolys_layer": None
        },
        reload=True
    )
    BM.load()
    assert os.path.exists(tmpdir.join("src", "dem_bc.tif"))
    assert os.path.exists(tmpdir.join("src", "dem_exbc.tif"))
    assert os.path.exists(tmpdir.join("src", "dem.tif"))


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
            "warm_high": [525, 875, 1400, 10000],
        }
        BM.data["elevation"] = pd.DataFrame(bad_elevation)
        BM.data["rulepolys"] = pd.DataFrame({"polygon_number": [1]})
        util.validate_data(BM.data)


def test_invalid_dem_path():
    with pytest.raises(ConfigValueError):
        BM = BECModel(TESTCONFIG)
        BM.update_config({"dem": "tests/data/dem_does_not_exit.tif"})


def test_local_dem(tmpdir):
    """Test loading dem from local file path
    """
    BM = BECModel(TESTCONFIG)
    BM.update_config({"dem": "tests/data/dem_ok.tif"})
    BM.load()


def test_run(tmpdir):
    """
    Check that outputs are created, properly structured and consistent
    (not necessarily correct!)
    """
    BM = BECModel(TESTCONFIG)
    BM.update_config({"temp_folder": str(tmpdir)})
    BM.update_config({"out_file": str(os.path.join(tmpdir, "bectest.gpkg"))})
    BM.update_config({"dem": "tests/data/dem_ok.tif"})
    BM.load()
    BM.model()
    BM.postfilter()
    BM.write()
    assert os.path.exists(tmpdir.join("00_dem.tif"))
    assert os.path.exists(tmpdir.join("02_aspect.tif"))
    assert os.path.exists(tmpdir.join("bectest.gpkg"))
    assert fiona.listlayers(os.path.join(tmpdir, "bectest.gpkg")) == ["becmodel"]
    with fiona.open(os.path.join(tmpdir, "bectest.gpkg")) as output:
        assert list(output.schema["properties"].keys()) == [
            "BGC_LABEL",
            "AREA_HECTARES",
        ]
    # check outputs
    df = gpd.read_file(str(os.path.join(tmpdir, "bectest.gpkg")))
    areas = df.groupby(["BGC_LABEL"])["AREA_HECTARES"].sum().round()
    assert list(areas) == [5156.0, 553.0, 3617.0, 7550.0, 1511.0, 5049.0]


def test_nowoodland(tmpdir):
    BM = BECModel(TESTCONFIG)
    BM.update_config({"temp_folder": str(tmpdir)})
    BM.update_config({"out_file": str(os.path.join(tmpdir, "bectest_nowoodland.gpkg"))})
    BM.update_config({"elevation": "tests/data/elevation_nowoodland.csv"}, reload=True)
    BM.load()
    BM.model()
    BM.postfilter()
    BM.write()


def test_nohigh(tmpdir):
    BM = BECModel(TESTCONFIG)
    BM.update_config({"temp_folder": str(tmpdir)})
    BM.update_config({"out_file": str(os.path.join(tmpdir, "bectest_nohigh.gpkg"))})
    BM.update_config({"elevation": "tests/data/elevation_nohigh.csv"}, reload=True)
    BM.load()
    BM.model()
    BM.postfilter()
    BM.write()
