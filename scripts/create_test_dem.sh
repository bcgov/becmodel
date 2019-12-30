# Example of creating a DEM from another source

# Note that the data sources listed in tdem_list.txt are
# internal to bcgov.

gdalbuildvrt gdalbuildvrt dem_index.vrt \
  -input_file_list tdem_list.txt

gdalwarp dem_index.vrt dem_robson.tif \
  -te 1318987.5 807387.5 1536487.5 1006587.5 \
  -r bilinear \
  -tr 50 50 \
  -co COMPRESS=DEFLATE