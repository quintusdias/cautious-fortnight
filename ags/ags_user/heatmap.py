"""
Generates heat map out of log files.

Prior IP addresses whos heatmap triggered the abusive user policy.

59.172.176.158:  china
54.242.157.143:  amazonaws (led back to ESRI)
23.22.70.107:  amazonaws 2 (led back to ESRI)
198.102.33.251:  ESRI
216.81.81.85:  DHS
216.81.94.72:  DHS
192.43.65.245:  John Deere
204.54.36.245:  John Deere
64.184.91.58: # indianafiber.net
74.122.16.29 arin.ncemc.com

"""
# Standard library imports
import collections
import datetime as dt
import gzip

# Third party imports
import apache_log_parser
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from . import consts

to_exclude = [
    '/arcgis/rest/info?f=json',
    '/arcgis/rest/services/watchWarn',
    '/arcgis/rest/login',
    '/arcgis/rest/static',
    '/arcgis/sdk',
]


class HeatMap(object):
    """
    Attributes
    ----------
    infile : str
        Input gzipped apache log file
    ip_address : str
        IP address in quad-dot format.
    line_parser : obj
        Parses apache log files NCSA extended log format.
    outfile_base : str
        Base for output files.  This includes a PNG, both Excel and CSV files,
        and an HDF5 pandas store.
    project : str
        Name of project (either 'idpgis' or 'nowcoast').
    """

    def __init__(self, infile, outfile_base, ip_address, project=None):

        self.infile = infile
        self.outfile_base = outfile_base
        self.ip_address = ip_address
        self.project = project

        fmt = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
        self.line_parser = apache_log_parser.make_parser(fmt)

        self.hits = collections.defaultdict(int)
        self.errors = collections.defaultdict(int)

        if self.project == 'nowcoast':
            services = consts.nowcoast_services
        else:
            services = consts.idpgis_services

        self.folders = set(x.split('/')[0] for x in services)
        self.services = [x.split('/')[1] for x in services]

        self.service_hits = {}
        for service in self.services:
            self.service_hits[service] = collections.defaultdict(int)

        self.single_ip_service_hits = {}
        for service in self.services:
            self.single_ip_service_hits[service] = collections.defaultdict(int)

    def run(self):
        with gzip.GzipFile(self.infile) as gz:
            for idx, line in enumerate(gz):
                if idx % 10000 == 0:
                    print(idx)
                data = self.line_parser(line.decode('utf-8'))

                request_url = data['request_url'].replace('//', '/')

                if not request_url.startswith('/arcgis'):
                    continue

                if any(request_url.startswith(item) for item in to_exclude):
                    continue

                parts = request_url.split('/')
                if len(parts) < 5:
                    continue

                if request_url.startswith('/arcgis/services'):
                    folder = parts[3]
                    service = parts[4]
                elif request_url.startswith('/arcgis/rest/directories'):
                    # No idea what this is.
                    #
                    # /arcgis/rest/directories/arcgisoutput/System/CachingTools_GPServer/System_CachingTools/DeleteMapCache.htm
                    continue
                elif request_url.startswith('/arcgis/rest/services'):
                    if len(parts) < 6:
                        # /arcgis/rest/services/NOS_Biogeo_Biomapper?f=json
                        # Folder, but no service
                        continue
                    folder = parts[4]
                    service = parts[5]
                elif request_url.endswith('.css'):
                    # Don't bother with something like
                    #
                    # /arcgis/manager/3552/css/esri/header.css
                    continue
                else:
                    print(request_url + ' unhandled')
                    continue

                if folder not in self.folders:
                    continue

                if '?' in service:
                    service = service.split('?')[0]

                date = data['time_received_datetimeobj']
                key = dt.time(date.hour, date.minute)
                status = int(data['status'])

                self.hits[key] += 1
                if status >= 500:
                    self.errors[key] += 1
                try:
                    if data['remote_host'] == self.ip_address:
                        self.single_ip_service_hits[service][key] += 1
                except KeyError:
                    # print(service, key)
                    # print(data)
                    pass
                try:
                    self.service_hits[service][key] += 1
                except KeyError:
                    pass

        # Construct a dataframe from the collection of dicts.
        # The data frame will have a column for server errors, all hits, plus
        # each individual service.
        lst = [
            ('500 Errors', self.errors),
            ('All Hits', self.hits),
        ]
        lst.extend([(service, self.service_hits[service])
                    for service in self.services])
        d = dict(lst)
        df_all = pd.DataFrame(d)

        # Replace NaNs with zero since these are counts of hits.
        df_all.fillna(value=0, inplace=True)

        lst = [(service, self.single_ip_service_hits[service])
               for service in self.services]
        d = dict(lst)
        df_ip_address = pd.DataFrame(d)

        self.save_heatmap(df_all[self.services], df_ip_address)
        self.save_excel_csv(df_all)
        self.save_hdf5(df_all, df_ip_address)

    def save_excel_csv(self, df):
        # Save as an excel spreadsheet.
        str_index = [str(item) for item in df.index]
        xlsx_df = pd.DataFrame(df, index=str_index)
        xlsx_df.to_excel(self.outfile_base + '.xlsx')

        df.to_csv(self.outfile_base + '.csv')

    def save_hdf5(self, df, df_ip):
        store = pd.HDFStore(self.outfile_base + '.h5')
        store['df'] = df
        store['df_ip'] = df_ip
        store.close()

    def save_heatmap(self, all_svcs, ip_address):
        """
        Construct the heat map, save it as a PNG.

        Parameters
        ----------
        all_svcs : pd.DataFrame
            table of all hits
        ip_address : pd.DataFrame
            table of hits for just the target IP address
        """
        intensity = np.zeros(all_svcs.shape, dtype=np.float64)
        for j, (index, row) in enumerate(all_svcs.iterrows()):
            if index in ip_address.index:
                # Both tables have hits for the minute specified by this index.
                # The intensity of the pixels in this row are the ratio of the
                # hits coming from the specific IP address divided by the
                # total number of hits.  If the IP address is responsible for
                # all hits, the pixel is white.
                intensity[j, :] = ip_address.loc[index] / all_svcs.loc[index]
            else:
                # The specific IP address has no hits for any service for this
                # specific minute.  The row of pixels should be black.
                intensity[j, :] = np.zeros((1, len(self.services)),
                                           dtype=np.float64)

        # scale to 0-255, save as a uint8 image.
        intensity = np.nan_to_num(intensity) * 255
        plt.imsave(self.outfile_base + '.png', intensity.astype(np.uint8))
