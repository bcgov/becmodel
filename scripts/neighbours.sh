# create the neighbours file

ogr2ogr \
  becmodel/data/neighbours.geojson \
  /vsizip//vsicurl/https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_1_states_provinces.zip \
  -f GeoJSON \
  -where "name in ('Washington','Idaho','Montana','Alberta','Yukon','Alaska','Northwest Territories')" \
  -spat -139.2, 48.9, -113.5, 60.2 \
  -clipsrc -139.2, 48.9, -113.5, 60.2