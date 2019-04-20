# Default config
# To overwrite these values, provide becmodel command with a config file

config = {
    "rulepolys_file": "tests/data/data.gdb.zip",
    "rulepolys_layer": "rule_polys",
    "elevation": "tests/data/elevation.csv",
    "becmaster": "tests/data/becmaster.csv",
    "cell_size": 50,
    "smoothing_tolerance": 50,
    "generalize_tolerance": 200,
    "parkland_removeal_threshold": 2500000,
    "noise_removal_threshold": 250000,
    "expand_bounds": 1000,
    "wksp": "becmodel_tempdata",
    "log_file": "becmodel.log",
    "flat_aspect_slope_threshold": 15,
    "majority_filter_steep_slope_threshold": 25,
    "majority_filter_low_slope_radius": 5,
    "majority_filter_steep_slope_radius": 3,

    # define aspects as list of dicts
    # each aspect has a 'code' and a list of valid ranges as degrees azimuth
    # (0-361, an extra degree to ensure full coverage)
    # *note* aspects are not configurable through the config file interface

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
