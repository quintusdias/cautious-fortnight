# Standard library imports
import http.client as httplib
import json
import pathlib
import pickle
import urllib

# 3rd party library imports
import pandas as pd
from lxml import etree

# Local imports
from .rest import AgsRestAdminBase
from .stats import TokenRetrievalError


class DailySummary(AgsRestAdminBase):
    """
    Attributes
    ----------
    project : str
        Either nowcoast or idpgis
    startTime, endTime : date
        Summarize the logs between these times.
    doc : elementtree
        Will constitute the HTML for the output.
    output : path or str
        If None, save no output.  Otherwise write the results to CSV file.
    """
    def __init__(self, project, site, tier, startTime, endTime, level, output):
        super().__init__()
        self.site = site
        self.project = project
        self.tier = tier
        self.level = level
        self.output = output

        self.startTime = startTime
        self.endTime = endTime

        path = ('/mnt/intra_wwwdev/ncep/ncepintradev/htdocs'
                '/ncep_common/nowcoast/ags_stats/logs')
        self.root = pathlib.Path(path) / project / site / tier

        self.root.mkdir(exist_ok=True, parents=True)

        self.servers = [
            server + '.ncep.noaa.gov'
            for server in self.config[site][project][tier]
        ]

        # start the document
        self.doc = etree.Element('html')
        self.body = etree.SubElement(self.doc, 'body')

        # Append a table of contents.
        div = etree.SubElement(self.body, 'div', id='toc')
        self.toc = etree.SubElement(div, 'ul', id='toc')

    def run(self):
        """
        Collect a dataframe for each service, concatenate them, save to file.
        """

        dfs = []
        for server in self.servers:
            print(server)
            dfs.append(self.retrieve_server_logs(server))
        df = pd.concat(dfs)
        df.sort_index(inplace=True)

        if self.output is not None:
            if self.output.endswith('csv'):
                df.to_csv(self.output)
            elif self.output.endswith('pkl'):
                with open(self.output, mode='wb') as f:
                    pickle.dump(df, f)

    def retrieve_server_logs(self, server):
        """
        Retrieve logs from the specified server via REST.
        """

        try:
            self.token = self.get_token(server)
        except TokenRetrievalError as e:
            print(f"Could not retrieve token for {server}.")
            print(repr(e))
            return

        log_query_url = "/arcgis/admin/logs/query"

        params = {
            'startTime': int(self.startTime.timestamp() * 1000),
            'endTime': int(self.endTime.timestamp() * 1000),
            'level': self.level,
            'token': self.token,
            'f': 'json',
            'pageSize': 10000,
            'filter': {
                'server': '*',
                'machines': [server],
            },
        }
        params = urllib.parse.urlencode(params)

        httpConn = httplib.HTTPConnection(server, self.ags_port)
        httpConn.request('POST', log_query_url, params, self.headers)
        response = httpConn.getresponse()
        if (response.status != 200):
            httpConn.close()
            msg = ("Error while fetching log info from the "
                   "admin URL.  Please check the URL and try again.")
            raise RuntimeError(msg)

        rawdata = response.read()
        data = json.loads(rawdata)

        # url = f"http://{server}:{self.ags_port}/arcgis/admin/logs/query"

        # # Supply the log level, filter, token, and return format
        # params = {
        #     'startTime': int(self.startTime.timestamp() * 1000),
        #     'endTime': int(self.endTime.timestamp() * 1000),
        #     'level': self.level,
        #     'token': self.token,
        #     'f': 'json',
        #     'pageSize': 10000,
        #     'filter': {
        #         'server': '*',
        #         'machines': [server],
        #     },
        # }

        # r = requests.post(url, params=params, headers=self.headers)
        # r.raise_for_status()

        # data = r.json()

        if data['hasMore']:
            print(f'{server} actually has more')

        df = pd.DataFrame(data['logMessages'])
        try:
            df['time'] = pd.to_datetime(df['time'], unit='ms')
        except KeyError:
            print(server, "No data")
            return

        return df.set_index('time')
