"""
Bin apache logs to the minute, measure certain metrics.

      i) all traffic
     ii) all errors
    iii) just server errors
     iv) hits (successful apache code)
      v) layerinfo requests
"""

# Standard library imports ...
import collections
import glob
import gzip
import os

# Third party imports...
import apache_log_parser
import pandas as pd

FMT = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""


class ApacheLogBinner(object):
    """
    Attributes
    ----------
    line_parser : function
        Parse line from apache log file in combined format.
    store : HDFStore
        Store binned data using pytables format here.
    all_traffic : dictionary
        Count all traffic, bin to minute.
    hits : dictionary
        Count successful hits, bin to minute.
    all_errors : dictionary
        Count all apache errors, bin to minute.
    layerinfo : dictionary
        Count layer info requests, bin to minute (nowcoast only)
    pointforecast : dictionary
        Count point forecast, bin to minute (nowcoast only)
    server_errors : dictionary
        Count only server errors, bin to minute.
    """

    def __init__(self, project=None, input_file=None, output_file=None):
        """
        project : str
            Either idpgis or nowcoast.
        input_file : str
            Path to apache log file.  Should only be used for testing.
        output_file : str
            Path to output HDF5 store.  Should only be used for testing.
        """

        self.project = project

        # Set up the input source.
        if input_file is None:
            # Probably running via cron.
            self.pattern = (f"{os.environ['HOME']}"
                            f"/data/logs/akamai/{project}/daily/*.gz")
        else:
            # Probably just testing.
            self.pattern = input_file

        self.line_parser = apache_log_parser.make_parser(FMT)

        self.all_errors = collections.defaultdict(int)
        self.all_traffic = collections.defaultdict(int)
        self.hits = collections.defaultdict(int)
        self.layerinfo = collections.defaultdict(int)
        self.pointforecast = collections.defaultdict(int)
        self.server_errors = collections.defaultdict(int)

        if output_file is None:
            # Probably running via cron.
            file = '{home}/data/logs/akamai/{project}/daily.h5'
            file = file.format(home=os.environ['HOME'], project=project)
        else:
            # Probably just testing.
            file = output_file
        self.store = pd.HDFStore(file)

    def __del__(self):
        self.store.close()

    def process_log_file(self, gzipped_log_file):

        print('processing ', gzipped_log_file)
        with gzip.GzipFile(gzipped_log_file, 'r') as gz:
            for line in gz:

                data = self.line_parser(line.decode('utf-8'))

                dt = data['time_received_datetimeobj']
                dt = dt.replace(second=0, microsecond=0)
                key = pd.Timestamp(dt)

                try:
                    if 'layerinfo' in data['request_url_path'].lower():
                        self.layerinfo[key] += 1
                    elif 'pointforecast' in data['request_url_path'].lower():
                        self.pointforecast[key] += 1
                except KeyError:
                    # This can happen if the request is corrupt.  Should result
                    # in a server error, so don't just skip it.
                    pass

                status = int(data['status'])

                self.all_traffic[key] += 1

                if status <= 400:
                    self.hits[key] += 1

                if status >= 400:
                    self.all_errors[key] += 1

                if status >= 500:
                    self.server_errors[key] += 1

    def run(self):

        for file in sorted(glob.glob(self.pattern))[-3:]:
            self.process_log_file(file)

        self.store['all_errors'] = pd.Series(self.all_errors)
        self.store['all_traffic'] = pd.Series(self.all_traffic)
        self.store['hits'] = pd.Series(self.hits)
        self.store['layerinfo'] = pd.Series(self.layerinfo)
        self.store['pointforecast'] = pd.Series(self.pointforecast)
        self.store['server_errors'] = pd.Series(self.server_errors)
