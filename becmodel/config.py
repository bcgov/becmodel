# Default config
# To overwrite these values, initialize becmodel with a config file

defaultconfig = {
    "rulepolys_file": "tests/data/data.gdb.zip",
    "rulepolys_layer": "rule_polys",
    "elevation": "tests/data/elevation.csv",
    "cell_size": 50,
    # cell connectivity accepts either rasterio (4/8) or skimage (1/2) style
    # https://scikit-image.org/docs/dev/api/skimage.morphology.html#label
    # https://rasterio.readthedocs.io/en/stable/api/rasterio.features.html#rasterio.features.shapes
    "cell_connectivity": 1,
    "high_elevation_removal_threshold": 2500000,
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
    # ---- Below are config items that are not configurable via config file.
    # ---- Changes to these values require changes to this file.
    # ----------------------------------------------------------------------
    # define areas to be aggregated/removed via 'parkland_removal_threshold'
    # Note that alpine comes from first four characters of beclabel
    # .str[:4]
    # parkland and woodland come from the seventh character of beclabel
    # .str[6:7]
    "high_elevation_removal_threshold_alpine": ["AT", "BAFA", "CMA", "IMA"],
    "high_elevation_removal_threshold_parkland": ["p", "s"],
    "high_elevation_removal_threshold_woodland": ["w"],
    # define aspects as list of dicts
    # each aspect has a 'code' and a list of valid ranges as degrees azimuth
    # (0-361, an extra degree to ensure full coverage)
    "aspects": [
        {
            "name": "cool",
            "code": 100,
            "ranges": [{"min": 0, "max": 45}, {"min": 315, "max": 361}],
        },
        {
            "name": "neutral",
            "code": 200,
            "ranges": [
                {"min": -1, "max": 0},  # -1 values are flat areas
                {"min": 45, "max": 135},
                {"min": 270, "max": 315},
            ],
        },
        {"name": "warm", "code": 300, "ranges": [{"min": 135, "max": 270}]},
    ],
}
