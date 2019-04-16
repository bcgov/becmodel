# Default config
# To overwrite these values, provide becmodel command with a config file

config = {
    "rulepolygon_file": "bec_inputs.gdb",
    "rulepolygon_layer": "rule_polys",
    "rulepolygon_table": "rulepolygon.csv",
    "elevation": "elevationrange.csv",
    "becmaster": "becmaster.csv",
    "cell_size": 50,
    "smoothing_tolerance": 50,
    "generalize_tolerance": 200,
    "parkland_removeal_threshold": 2500000,
    "noise_removal_threshold": 250000,
    "expand_bounds": 1000,
    "wksp": "becmodel_tempdata",
    "log_file": "becmodel.log"
}
