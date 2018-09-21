# Standard library imports
import datetime as dt
import http.client as httplib
import json
import pathlib
import urllib

# 3rd party library imports
import pandas as pd
from lxml import etree
import matplotlib.pyplot as plt

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
    df : pandas dataframe
        Dataframe of the AGS logs
    """
    def __init__(self, project, site, tier, outfile, time, level):
        super().__init__()
        self.site = site.upper()
        self.project = project
        self.tier = tier
        self.level = level

        self.startTime, self.endTime = time
        self.outfile = pathlib.Path(outfile)
        self.root = self.outfile.parents[0]

        self.servers = [
            server + '.ncep.noaa.gov'
            for server in self.config[self.site][self.project][self.tier]
        ]

        self.setup_output()

    def setup_output(self):

        self.root.mkdir(exist_ok=True, parents=True)

        # start the document
        self.doc = etree.Element('html')

        # Add some CSS.
        header = etree.SubElement(self.doc, 'head')

        # Make the document refresh every 60 seconds.
        meta = etree.SubElement(header, 'meta')
        meta.attrib['http-equiv'] = 'refresh'
        meta.attrib['content'] = '60'

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
        self.df = df

        self.write_output(df)

    def write_hdf5(self, df):
        """
        Store the dataframe as an HDF5 file that we can access later if
        necessary.  Then link it into the output HTML.
        """
        # First just save the data so we can get it later.
        with pd.HDFStore(self.outfile) as store:
            store['df'] = df

        # Link to the HDF5 file.
        div = etree.SubElement(self.body, 'div')
        p = etree.SubElement(div, 'p')
        p.text = 'The error messages are stored in a pandas dataframe ('
        a = etree.SubElement(p, 'a', href=f"self.outfile.stem")
        a.text = 'latest.h5'
        a.tail = ').  To read after downloading, try the following:'
        pre = etree.SubElement(div, 'pre')
        pre.text = (
            f">>> import pandas as pd\n"
            f">>> with pd.HDFStore('{self.outfile.stem}') as store: "
            f"df = store['df']"
        )

    def write_output(self, df):
        """
        Create the output document content.
        """

        self.write_hdf5(df)
        self.write_daily_summary_by_vm_and_code(df)
        self.write_hourly_summary(df)

        # Last thing is to just write the HTML to file.
        self.doc.getroottree().write(str(self.root / 'index.html'),
                                     encoding='utf-8', pretty_print=True)

    def write_hourly_summary(self, df):
        """
        Summarize by the hour.
        """
        # Upon counting, all the columns count the same things, so just take
        # any column.  Choose elapsed for no particular reason.
        #
        # This used to work prior to v0.23.0
        # df = df.groupby([df.index.day, df.index.hour]).count()
        #
        # Now we have to be a bit more deliberate.
        idx = pd.MultiIndex.from_arrays([df.index.day, df.index.hour],
                                        names=['day', 'hour'])
        df.index = idx
        df = df.groupby(df.index).count()

        # Reset the index to be a normal by-the-hour index instead of a
        # multi-index.  That way the datetime information comes out on the
        # x-axis instead of "time,time".
        values = [
            dt.datetime(self.startTime.year, self.startTime.month,
                        day, hour, 0, 0)
            for day, hour in df.index
        ]
        df.index = pd.DatetimeIndex(values)

        fig, ax = plt.subplots()
        df['code'].plot(ax=ax)
        ax.set_title('Total Errors')

        path = self.root / 'hourly_summary.png'
        fig.savefig(str(path))

        etree.SubElement(self.body, 'hr')
        a = etree.SubElement(self.body, 'a', name='summary_by_hour')
        div = etree.SubElement(self.body, 'div', id='by_hour', name='by_hour')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Summary by Hour'
        etree.SubElement(div, 'img', src=path.name)

        # Link the DIV into the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#summary_by_hour')
        a.text = 'Summary by Hour'

    def write_daily_summary_by_vm_and_code(self, df):
        """
        Summarize by VM and by code.
        """
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
        a = etree.SubElement(self.body, 'a', name='summary_by_vm')
        div = etree.SubElement(self.body, 'div', id='by_vm', name='by_vm')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Summary by VM'

        h2 = etree.SubElement(div, 'h2')
        h2.text = (f"Date Range: "
                   f"{self.startTime.strftime('%Y-%m-%dT%H:%M')} - "
                   f"{self.endTime.strftime('%Y-%m-%dT%H:%M')}")

        div.append(table)

        # Link the DIV into the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#summary_by_vm')
        a.text = 'Summary by VM'

    def retrieve_server_logs(self, server):
        """
        Retrieve logs from the specified server via REST.
        """

        try:
            self.token = self.get_token(server)
        except (TokenRetrievalError, ConnectionRefusedError) as e:
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

        lst = []
        count = 0
        while True:
            # Loop until arcgis server says it's done.
            encoded_params = urllib.parse.urlencode(params)

            httpConn = httplib.HTTPConnection(server, self.ags_port)
            httpConn.request('POST', log_query_url, encoded_params,
                             self.headers)
            response = httpConn.getresponse()
            if (response.status != 200):
                httpConn.close()
                msg = ("Error while fetching log info from the "
                       "admin URL.  Please check the URL and try again.")
                raise RuntimeError(msg)

            rawdata = response.read()
            data = json.loads(rawdata)

            # msg = "Retrieved [{} - {}]"
            # print(msg.format(dt.datetime.fromtimestamp(data['endTime']/1000),
            #                  dt.datetime.fromtimestamp(data['startTime']/1000)))

            df = pd.DataFrame(data['logMessages'])
            try:
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            except KeyError:
                print(server, "No data")
                return

            lst.append(df.set_index('time'))

            if not data['hasMore']:
                break

            # Ok there is more.  According to
            #
            # http://resources.arcgis.com/en/help/server-admin-api/logsQuery.html
            #
            # to get the next set of records, pass the "endTime" member as
            # the "startTime" parameter for the next request. Time can be
            # specified in milliseconds since UNIX epoch, or as an ArcGIS
            # Server timestamp.
            #
            # Not true!
            #
            # params['startTime'] = data['endTime']
            params['startTime'] = data['startTime']
            print(f"{count} ", end='')

        # Concatenate the dataframes.
        df = pd.concat(lst)
        return df


def get_logs(project='nowcoast', site='bldr', tier='op',
             startTime=None, endTime=None, level='WARNING'):
    """
    Simple function for retrieving AGS logs.

    Parameters
    ----------
    startTime, endTime : datetimes
        If both are none, they default to specifying the last six hours.

    Return Value
    ------------
    pandas dataframe of the log items
    """

    if startTime is None and endTime is None:
        endTime = dt.datetime.now()
        startTime = endTime - dt.timedelta(hours=6)

    obj = SummarizeAgsLogs(project, site, tier, startTime, endTime, level)
    obj.run()
    return obj.df
