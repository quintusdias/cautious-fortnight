# Standard library imports
import gzip

# 3rd party library imports
import apache_log_parser
import pyproj


class NotValidWMSError(IOError):
    """
    Raise this exception when we either do not have a WMS request or the WMS
    request has something wrong with it.
    """
    pass


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

    def process_line(self, line):
        """
        Extract the width and height of the box in pixels.  Also extract the
        bounding box.
        """
        data = self.parser(line.decode('utf-8'))

        # Only keep successful requests.
        if int(data['status']) >= 400:
            raise NotValidWMSError("HTTP error code")

        params = {
            x.lower(): [
                y.lower()
                for y in data['request_url_query_dict'][x]
            ]
            for x in data['request_url_query_dict'].keys()
        }

        if 'getmap' not in params['request']:
            raise NotValidWMSError("Possibly valid WMS, but not a map draw.")

        crs = params['crs'][0]

        width = int(params['width'][0])
        height = int(params['height'][0])

        bbox = params['bbox'][0]

        coords = bbox.split(',')
        if len(coords) != 4:
            raise NotValidWMSError("Not a valid bounding box.")

        return {
            'width': width,
            'height': height,
            'bbox': bbox,
            'layers': params['layers'][0],
            'crs': params['crs'][0],
            'version': params['version'][0],
        }

    def run(self):

        wms_count = 0

        with open(self.output_csv_file, mode='wt') as of:
            with gzip.GzipFile(self.apache_log_file) as gz:
                for idx, line in enumerate(gz):

                    if idx % 10000 == 0:
                        print(f"{idx:<8}:{wms_count:<8} {line.decode('utf-8')}")

                    try:
                        params = self.process_line(line)
                    except:
                        continue
                    else:
                        wms_count += 1

                    of.write((
                        f"{params['crs']}|"
                        f"{params['version']}|"
                        f"{params['width']}|"
                        f"{params['height']}|"
                        f"{params['layers']}|"
                        f"{params['bbox']}\n"
                    ))
