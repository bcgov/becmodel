# Default config
# To overwrite these values, initialize becmodel with a config file

defaultconfig = {
    "rulepolys_file": "tests/data/data.gdb.zip",
    "rulepolys_layer": "rule_polys",
    "elevation": "tests/data/elevation.csv",
    "cell_size": 50,
    "smoothing_tolerance": 50,                      # not used
    "generalize_tolerance": 200,                    # not used
    "parkland_removal_threshold": 2500000,
    "noise_removal_threshold": 250000,
    "expand_bounds": 1000,
    "wksp": "becmodel_tempdata",
    "out_file": "becmodel.gpkg",
    "out_layer": "becvalue",
    "log_file": "becmodel.log",
    "flat_aspect_slope_threshold": 15,
    "majority_filter_steep_slope_threshold": 25,
    "majority_filter_low_slope_size": 250,
    "majority_filter_steep_slope_size": 150,


    # ----------------------------------------------------------------------
    # ---- config items that are not configurable through a config file,
    # ---- changes to these values require updates to this file.
    # ----------------------------------------------------------------------

    # define areas to be aggregated/removed via 'parkland_removal_threshold'
    "parkland_removal_threshold_zones": ["BAFA", "CMA", "IMA"],
    "parkland_removal_threshold_descriptors": ["p", "s", "w"],

    # define aspects as list of dicts
    # each aspect has a 'code' and a list of valid ranges as degrees azimuth
    # (0-361, an extra degree to ensure full coverage)
    "aspects": [
        {
            "name": "cool",
            "code": 100,
            "ranges": [
                {"min": 0, "max": 45},
                {"min": 315, "max": 361}
            ],
        },
        {
            "name": "neutral",
            "code": 200,
            "ranges": [
                {"min": -1, "max": 0},    # -1 values are flat areas
                {"min": 45, "max": 135},
                {"min": 270, "max": 315}
            ],
        },
        {
            "name": "warm",
            "code": 300,
            "ranges": [
                {"min": 135, "max": 270}
            ]
        },
    ],
}
