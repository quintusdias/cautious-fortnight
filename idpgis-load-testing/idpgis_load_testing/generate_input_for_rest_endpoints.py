# Standard library imports
import gzip
import pathlib

# 3rd party library imports
import apache_log_parser
import pyproj


class GenerateRESTinput(object):
    """
    Generate input for JMeter to throw at the REST endpoints.

    Attributes
    ----------
    apache_log_file : path or str
        Gzipped apache log file for input.
    output_csv_file : path or str
        Format is {width}|{height}|{bbox}|{layers}
    parser :
        Parses the apache log file.
    wgs84 : epsg3857 :
        Transform the input from EPSG:3857 to EPSG:4326
    """
    def __init__(self, apache_log_file, output_directory):
        self.apache_log_file = apache_log_file
        self.output_root = pathlib.Path(output_directory)

        fmt = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
        self.parser = apache_log_parser.make_parser(fmt)

        self.wgs84 = pyproj.Proj(init='EPSG:4326')
        self.epsg3857 = pyproj.Proj(init='EPSG:3857')

    def run(self):

        success_count = 0
        export_count = 0
        fp = {}

        with gzip.GzipFile(self.apache_log_file) as gz:
            for idx, line in enumerate(gz):

                data = self.parser(line.decode('utf-8'))

                # What service?
                parts = data['request_url_path'].split('/')
                if len(parts) < 8:
                    # URL path does not have enough parts.
                    continue

                try:
                    folder, service, svtype = parts[5:8]
                except Exception as e:
                    # Not a long enough path so skip it.
                    continue

                # Force to be either "ImageServer" or "MapServer"
                if svtype.lower() == 'imageserver':
                    svtype = 'ImageServer'
                elif svtype.lower() == 'mapserver':
                    svtype = 'MapServer'
                else:
                    continue

                name = f"{folder}/{service}/{svtype}"

                # Only keep successful requests.
                if int(data['status']) >= 400:
                    continue

                if idx % 10000 == 0:
                    print(idx, export_count, success_count)
                    print(line.decode('utf-8'))

                # Clean up the parameters so we can see what we are doing.
                params = {
                    x.lower(): [
                        y.lower()
                        for y in data['request_url_query_dict'][x]
                    ]
                    for x in data['request_url_query_dict'].keys()
                }

                export_count += 1

                if 'bbox' not in params.keys():
                    # no point to a request like this!!!
                    continue

                if 'bboxsr' in params.keys():
                    bboxsr = params['bboxsr'][0]
                else:
                    # Must be invalid???
                    continue
                if 'wkt' in bboxsr:
                    # Jeezus, don't bother with these.
                    continue

                bbox = params['bbox'][0]

                if 'size' in params.keys():
                    try:
                        width, height = params['size'][0].split(',')
                    except ValueError:
                        continue
                else:
                    if 'width' in params.keys():
                        width = params['width'][0]
                        height = params['height'][0]
                    else:
                        continue

                if 'layers' not in params.keys():
                    continue
                    raise RuntimeError(line.decode('utf-8'))
                else:
                    layers = params['layers'][0]

                x1, y1, x2, y2 = [float(x) for x in bbox.split(',')]
                bbox = f"{x1:.4f},{y1:.4f},{x2:.4f},{y1:.4f}"

                # If we don't already know about it, open a new file for
                # this.
                if name not in fp.keys():
                    output_str = f"{str(self.output_root)}/{name}.csv"
                    p = pathlib.Path(output_str)
                    p.parent.mkdir(parents=True, exist_ok=True)
                    fp[name] = p.open(mode='wt')

                fp[name].write(f"{width}|{height}|{bbox}|{bboxsr}|{layers}\n")

                success_count += 1
