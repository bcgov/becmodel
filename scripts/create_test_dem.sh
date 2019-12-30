# Example of creating a DEM from another source

# Create a vrt from multiple source files
# *Note* files listed in tdem_list.txt are internal to bcgov
gdalbuildvrt gdalbuildvrt dem_index.vrt \
  -input_file_list tdem_list.txt

# Mosaic the files listed in the vrt into a single file.
# As source files are already BC Albers, we just need to specify the exact
# output extent to get things to line up correctly
gdalwarp dem_index.vrt dem_robson.tif \
  -te 1318987.5 807387.5 1536487.5 1006587.5 \
  -r bilinear \
  -tr 50 50 \
  -co COMPRESS=DEFLATE