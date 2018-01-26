# Standard library imports
import http.client as httplib
import json
import urllib

# 3rd party library imports
import pandas as pd
from lxml import etree

# Local imports
from .rest import AgsRestAdminBase
from .stats import TokenRetrievalError


class SummarizeAgsLogs(AgsRestAdminBase):
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

        self.root = output

        self.root.mkdir(exist_ok=True, parents=True)

        self.servers = [
            server + '.ncep.noaa.gov'
            for server in self.config[site][project][tier]
        ]

        # start the document
        self.doc = etree.Element('html')

        # Add some CSS.
        header = etree.SubElement(self.doc, 'head')
        style = etree.SubElement(header, 'style', type='text/css')

        # Write the global table styles.  We do this here instead of using
        # the dataframe option because we only need do it once.
        style.text = (
            "\n"
            "table {\n"
            "    border-collapse: collapse;\n"
            "}\n"
            "table td {\n"
            "    border-right: 1px solid #99CCCC;\n"
            "    border-bottom:  1px solid #99CCCC;\n"
            "    text-align:  right;\n"
            "}\n"
            "table th {\n"
            "    border-right: 3px solid #99CCCC;\n"
            "    border-bottom:  1px solid #99CCCC;\n"
            "    padding-right:  .3em;\n"
            "}\n"
            "table th[scope=\"col\"] {\n"
            "    border-right: 1px solid #99CCCC;\n"
            "    border-bottom:  3px solid #99CCCC;\n"
            "    padding-right:  .3em;\n"
            "}\n"
            "table th[scope=\"col\"]:first-child {\n"
            "    border-right: 3px solid #99CCCC;\n"
            "}\n"
            "table th[scope=\"col\"]:last-child {\n"
            "    border-right: 0;\n"
            "}\n"
            "table tr:last-child th {\n"
            "    border-bottom: 0;\n"
            "}\n"
        )

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

        self.write_output(df)

    def write_output(self, df):

        # First just save the data so we can get it later.
        with pd.HDFStore(self.root / 'latest.h5') as store:
            store['df'] = df

        # Summarize by VM and by code.
        # Upon counting, all the columns count the same things, so just take
        # any column.  Choose elapsed for no particular reason.
        df = df.groupby(['machine', 'code']).count()['elapsed'].unstack()

        # Add a nan-aware sum as the final columndf.
        df['total errors'] = df.sum(axis=1)

        # All of the columns count the same thing, so just take any column.
        txt = df.style.set_caption('Errors By VM').render()
        doc = etree.HTML(txt)
        table = doc.xpath('body/table')[0]

        etree.SubElement(self.body, 'hr')
        div = etree.SubElement(self.body, 'div', id='by_vm', name='by_vm')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Summary by VM'

        h2 = etree.SubElement(div, 'h2')
        h2.text = (f"Date Range: "
                   f"{self.startTime.strftime('%Y-%m-%dT%H:%M')} - "
                   f"{self.endTime.strftime('%Y-%m-%dT%H:%M')}")

        div.append(table)

        self.doc.getroottree().write(str(self.root / 'index.html'),
                                     encoding='utf-8', pretty_print=True)

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
