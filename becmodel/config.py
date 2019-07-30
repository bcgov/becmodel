# Default config
# To overwrite these values, initialize becmodel with a config file

defaultconfig = {
    "rulepolys_file": "becmodel.gdb",
    "rulepolys_layer": "rulepolys",
    "elevation": "elevation.xls",
    "temp_folder": "tempdata",
    "out_file": "becmodel.gpkg",
    "out_layer": "becmodel",
    "cell_size_metres": 50,
    "cell_connectivity": 1,
    "dem_prefilter": False,
    "noise_removal_threshold_ha": 10,
    "high_elevation_removal_threshold_ha": 100,
    "neutral_aspect_slope_threshold_percent": 15,
    "majority_filter_steep_slope_threshold_percent": 25,
    "majority_filter_size_slope_low_metres": 250,
    "majority_filter_size_slope_steep_metres": 150,
    "expand_bounds_metres": 2000,
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
                {"min": 45, "max": 135},
                {"min": 270, "max": 315},
                {"min": 999, "max": 1000},  # 999 for flat areas
            ],
        },
        {"name": "warm", "code": 300, "ranges": [{"min": 135, "max": 270}]},
    ],
}
