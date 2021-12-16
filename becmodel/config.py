# Default config
# To overwrite these values, initialize becmodel with a config file
import tempfile

defaultconfig = {
    "rulepolys_file": "becmodel.gdb",
    "rulepolys_layer": "rulepolys",
    "elevation": "elevation.xls",
    "becmaster": None,
    "dem": None,
    "temp_folder": tempfile.mkdtemp(prefix="becmodel-"),
    "out_file": "becmodel.shp",
    "out_layer": "becmodel",
    "cell_size_metres": 50,
    "cell_connectivity": 1,
    "noise_removal_threshold_ha": 10,
    "high_elevation_removal_threshold_ha": 100,
    "aspect_neutral_slope_threshold_percent": 15,
    "aspect_midpoint_cool_degrees": 0,
    "aspect_midpoint_neutral_east_degrees": 90,
    "aspect_midpoint_warm_degrees": 200,
    "aspect_midpoint_neutral_west_degrees": 290,
    "majority_filter_steep_slope_threshold_percent": 25,
    "majority_filter_size_slope_low_metres": 250,
    "majority_filter_size_slope_steep_metres": 150,
    "expand_bounds_metres": 2000,
    # Areas to be aggregated/removed via 'high_elevation_removal_threshold'
    # to find Alpine, match beclabel first four characters
    # to find Parkland and Woodland, match beclabel seventh character
    "high_elevation_removal_threshold_alpine": ["BAFA", "CMA", "IMA"],
    "high_elevation_removal_threshold_parkland": ["p", "s"],
    "high_elevation_removal_threshold_woodland": ["w"],
}
