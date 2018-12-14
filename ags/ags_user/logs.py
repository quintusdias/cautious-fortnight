# Standard library imports
from dataclasses import dataclass
import datetime as dt
import http.client
import json
import pathlib
import urllib

# 3rd party library imports
import pandas as pd
from lxml import etree
import matplotlib.pyplot as plt
import requests

# Local imports
from .rest import AgsRestAdminBase


@dataclass
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
    outfile : str
        Write output to pandas HDF5 store file.
    html : bool
        If True, write an HTML summary into the root directory of the output
        file.
    df : pandas dataframe
        Dataframe of the AGS logs
    """
    tier: str
    html: bool
    outfile: str = None
    time: list = None
    level: str = 'WARNING'
    protocol: str = 'http'
    port: int = 6080
    server: str = None
    general: bool = False
    services: bool = False

    def __post_init__(self):
        super().__post_init__()

        if not self.general and not self.services:
            msg = (
                "At least one of either general or services log acquisition "
                "must be specified."
            )
            raise RuntimeError(msg)

        if self.level is None:
            self.level = 'WARNING'

        if self.outfile is None:
            self.outfile = (
                f"/mnt/intra_wwwdev/ncep/ncepintradev/htdocs/ncep_common"
                f"/nowcoast/ags_logs/{self.project}/logs.h5"
            )

        if self.time is None:
            self.time = [
                dt.datetime.now() - dt.timedelta(hours=24), dt.datetime.now()
        ]

        self.startTime, self.endTime = self.time
        self.outfile = pathlib.Path(self.outfile)
        self.root = self.outfile.parents[0]

        self.servers = [
            server + '.ncep.noaa.gov'
            for server in self.config[self.site][self.project][self.tier]
        ]

        if self.server is not None:
            # Ok, throw the server list out and just restrict to this one.
            self.servers = [self.server + '.ncep.noaa.gov']


        self.setup_output()

    def setup_output(self):

        if not self.html:
            return

        self.root.mkdir(exist_ok=True, parents=True)

        # start the document
        self.doc = etree.Element('html')

        # Add some CSS.
        self.head = etree.SubElement(self.doc, 'head')

        # Make the document refresh every 60 seconds.
        meta = etree.SubElement(self.head, 'meta')
        meta.attrib['http-equiv'] = 'refresh'
        meta.attrib['content'] = '60'

        self.table_styles = [
            # checkerboard pattern in the interior
            dict(selector='td',
                 props=[('border-right', '1px solid #99CCCC'),
                        ('border-bottom', '1px solid #99CCCC'),
                        ('text-align', 'right')]),
            # the header elements should also have lower and right borders
            dict(selector='th',
                 props=[('border-right', '3px solid #99CCCC'),
                        ('border-bottom', '1px solid #99CCCC'),
                        ('text-align', 'center'),
                        ('padding-right', '.3em')]),
            dict(selector='th.col_heading',
                 props=[('border-right', '1px solid #99CCCC'),
                        ('border-bottom', '3px solid #99CCCC')]),
            dict(selector='th.blank',
                 props=[('border-right', '3px solid #99CCCC'),]),
            # take the bottom and right border off the bottom and right cells
            dict(selector='th.col_heading:last-child',
                 props=[('border-right', '0')]),
            dict(selector='tbody tr:last-child th',
                 props=[('border-bottom', '0')]),
            # remove the bottom borders of the last row
            dict(selector='tbody tr:last-child td',
                 props=[('border-bottom', '0')]),
            dict(selector='td:last-child',
                 props=[('border-right', '0')]),
        ]

        self.body = etree.SubElement(self.doc, 'body')

        # Append a table of contents.
        div = etree.SubElement(self.body, 'div', id='toc')
        self.toc = etree.SubElement(div, 'ul', id='toc')

    def collect_general_logs(self):

        dfs = []
        for server in self.servers:
            print(server)
            dfs.append(self.retrieve_server_logs(server))

        df = pd.concat(dfs)
        df.sort_index(inplace=True)
        self.df_general = df

    def collect_service_logs(self):

        self._df_list = []

        for server in self.servers:
            print(server)
            try:
                self.services = self._get_services(server)
            except requests.exceptions.ConnectionError:
                # server is down? op5a?
                continue
            self.token = self.get_token(server)

            self.query_services_logs(server)

        self.df_services = pd.concat(self._df_list)

    def query_services_logs(self, server):

        for service in self.services:
            print(service)
            self.query_service_logs(server, service)

    def query_service_logs(self, server, service):

        path = '/arcgis/admin/logs/query'
        url = f'{self.protocol}://{server}:{self.port}{path}'

        params = {
            'startTime': int(self.time[0].timestamp() * 1000),
            'endTime': int(self.time[1].timestamp() * 1000),
            'level': self.level,
            'token': self.token,
            'f': 'json',
            'pageSize': 500,
            'sinceLastStart': True,
            'filter': {
                'services': [service],
                'machines': [server],
            }
        }

        encoded_params = urllib.parse.urlencode(params)
        conn = http.client.HTTPConnection(server, self.port)

        while True:
            conn.request('POST', path, encoded_params, self.headers)
            response = conn.getresponse()
            if response.status != 200:
                msg = (
                    "Error while fetching logs from the admin URL.  "
                    "Please check the URL and try again."
                )
                raise RuntimeError(msg)

            rawdata = response.read()
            data = json.loads(rawdata)

            df = pd.DataFrame(data['logMessages'])
            if len(df) == 0:
                # No data, so we're done.
                return

            df['time'] = pd.to_datetime(df['time'], unit='ms')

            self._df_list.append(df)

            if data['hasMore']:
                # Must get the next set of records.
                pass
            else:
                break

    def _get_services(self, server):
        """
        Get list of all services.

        Returns
        -------
        list
            List of services.
        """
        service_list = []

        params = {'f': 'json'}
        url = f"{self.protocol}://{server}:{self.port}/arcgis/rest"
        r = requests.get(url, params=params)
        r.raise_for_status()
        directory_json = r.json()

        for folder in directory_json['folders']:
            url = f"http://{server}:{self.port}/arcgis/rest/services/{folder}"
            r = requests.get(url, params=params, verify=False)
            r.raise_for_status()
            service_json = r.json()

            for item in service_json['services']:
                service_list.append(f"{item['name']}.{item['type']}")

        return service_list

    def run(self):
        """
        Collect a dataframe for each service, concatenate them, save to file.
        """
        if self.general:
            self.collect_general_logs()

        if self.services:
            self.collect_service_logs()

        self.write_output()

    def write_hdf5(self):
        """
        Store the dataframe as an HDF5 file that we can access later if
        necessary.  Then link it into the output HTML.
        """
        # First just save the data so we can get it later.
        with pd.HDFStore(self.outfile) as store:
            if self.general:
                store['general'] = self.df_general

            if self.services:
                store['services'] = self.df_services

        # Link to the HDF5 file.
        if self.html:
            div = etree.SubElement(self.body, 'div')
            p = etree.SubElement(div, 'p')
            p.text = 'The error messages are stored in a pandas dataframe ('
            a = etree.SubElement(p, 'a', href=f"{self.outfile.stem}")
            a.text = 'logs.h5'
            a.tail = ').  To read after downloading, try the following:'
            pre = etree.SubElement(div, 'pre')
            pre.text = (
                f">>> import pandas as pd\n"
                f">>> store = pd.HDFStore('{self.outfile.stem}')"
            )

    def write_output(self):
        """
        Create the output document content.
        """

        self.write_hdf5()
        
        if self.html:
            self.write_daily_summary_by_vm_and_code(self.df_general)
            self.write_hourly_summary(self.df_general)
            self.write_service_summary()

            # Last thing is to just write the HTML to file.
            file = str(self.root / 'index.html')
            roottree = self.doc.getroottree()
            roottree.write(file, encoding='utf-8', pretty_print=True)

    def write_service_summary(self):
        """
        Summarize by service.
        """
        if self.project == 'nowcoast':
            return

        s = self.df_services.groupby('source')['code'].count().sort_values(ascending=False).head(n=10)
        s.name = 'Errors'

        html = s.to_frame().style.set_table_styles(self.table_styles).render()
        table = etree.HTML(html)

        etree.SubElement(self.body, 'hr')

        a = etree.SubElement(self.body, 'a', name='summary_by_service')
        div = etree.SubElement(self.body, 'div', id='by_service', name='by_service')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Summary by Service'

        self.body.append(table)

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
        txt = (df.style
                 .set_table_styles(self.table_styles)
                 .set_caption('Errors By VM')
                 .render())
        doc = etree.HTML(txt)

        # Get the <STYLE> element and stick it in the correct place.
        style = doc.xpath('head/style')[0]
        self.head.append(style)

        # And now get the table.
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
        except (RuntimeError, ConnectionRefusedError) as e:
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

            conn = http.client.HTTPConnection(server, self.ags_port)
            conn.request('POST', log_query_url, encoded_params,
                         self.headers)
            response = conn.getresponse()
            if (response.status != 200):
                conn.close()
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
