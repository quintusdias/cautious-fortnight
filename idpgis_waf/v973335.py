code = 973335
description = 'IE XSS Filters - Attack Detected'
uri = 'https://idpgis.ncep.noaa.gov/arcgis/rest/services/NWS_Observations/ahps_riv_gauges/MapServer/0/query'
query_string = 'f=json&returnIdsOnly=true&returnCountOnly=true&where=(status%20%3D%20%27action%27%20OR%20status%20%3D%20%27minor%27%20OR%20status%20%3D%20%27moderate%27%20OR%20status%20%3D%20%27major%27)%20AND%20(1%3D1)&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100'
