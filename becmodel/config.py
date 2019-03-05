# Default config
# To overwrite these values, provide becmodel command with a config file

config = {
    "rule_polygons_file": "becdata/inputs.gdb",
    "rule_polyons_layer": "rule_polys",
    "cell_size": 50,
    "smoothing_tolerance": 50,
    "generalize_tolerance": 200,
    "parkland_removeal_threshold": 2500000,
    "noise_removal_threshold": 250000,
    "expand_bounds": 1000,
    "wksp": "becdata",
    "log_file": "becmodel.log"
}
