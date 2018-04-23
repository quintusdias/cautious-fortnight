# Standard library imports
import gzip

# 3rd party library imports
import apache_log_parser
import pyproj


class GenerateWMSinput(object):
    """
    """
    def __init__(self, apache_log_file, output_csv_file):
        self.apache_log_file = apache_log_file
        self.output_csv_file = output_csv_file

        fmt = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
        self.parser = apache_log_parser.make_parser(fmt)

        self.wgs84 = pyproj.Proj(init='EPSG:4326')
        self.epsg3857 = pyproj.Proj(init='EPSG:3857')

    def run(self):

        success_count = 0
        wms_count = 0

        with open(self.output_csv_file, mode='wt') as of:
            with gzip.GzipFile(self.apache_log_file) as gz:
                for idx, line in enumerate(gz):

                    data = self.parser(line.decode('utf-8'))

                    # Only keep successful requests.
                    if int(data['status']) >= 400:
                        continue

                    if idx % 10000 == 0:
                        print(idx, wms_count, success_count)
                        print(line.decode('utf-8'))

                    params = {
                        x.lower(): [
                            y.lower()
                            for y in data['request_url_query_dict'][x]
                        ]
                        for x in data['request_url_query_dict'].keys()
                    }

                    try:
                        if 'getmap' not in params['request']:
                            continue
                    except KeyError:
                        continue

                    wms_count += 1

                    if 'bbox' not in params.keys():
                        # no point to a request like this!!!
                        continue

                    # We'll just do version 1.3.0 of WMS.
                    version = params['version'][0]
                    if not version.startswith('1.3'):
                        continue

                    if 'crs' in params.keys():
                        crs = params['crs'][0]
                    # elif 'srs' in params.keys():
                    #     crs = params['srs'][0]
                    else:
                        # Must be invalid???
                        continue

                    width = int(params['width'][0])
                    height = int(params['height'][0])
                    bbox = params['bbox'][0]

                    # if crs not in
                    # ['crs:84', 'epsg:4326', 'epsg:3857', 'epsg:4269']:
                    if crs not in ['crs:84', 'epsg:4326', 'epsg:3857']:
                        continue
                        # raise RuntimeError(line.decode('utf-8'))

                    if crs == 'epsg:3857':
                        # Transform into WGS84 but remember that for WMS 1.3.0,
                        # the bounding box ordering must be y/x.
                        x1, y1, x2, y2 = [float(x) for x in bbox.split(',')]
                        lon1, lat1 = pyproj.transform(self.epsg3857,
                                                      self.wgs84,
                                                      x1, y1)
                        lon2, lat2 = pyproj.transform(self.epsg3857,
                                                      self.wgs84,
                                                      x2, y2)
                        bbox = f"{lat1},{lon1},{lat2},{lon2}"

                    of.write(f"{width}|{height}|{bbox}\n")

                    success_count += 1
