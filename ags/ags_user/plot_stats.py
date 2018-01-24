# Standard library imports
import datetime as dt
import logging
import pathlib

# Third party library imports
from lxml import etree
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from .stats import ToolsBase

_DEFAULT_WEB_ROOT = ('/mnt/intra_wwwdev/ncep/ncepintradev/htdocs'
                     '/ncep_common/nowcoast')


class AGSServiceStatisticsPlotsViaMPL(ToolsBase):
    """
    Instance plots via MPL
    """

    def __init__(self, site, project, num_hours, web_root=_DEFAULT_WEB_ROOT):
        super().__init__(site, project)

        self.root = pathlib.Path(web_root) / 'ags_stats' / site \
            / self.project / 'mpl'

        self.num_hours = num_hours
        if self.num_hours <= 24:
            self.priority = 1
        else:
            self.priority = 2

        # Create the root directory if it does not already exist.
        self.root.mkdir(parents=True, exist_ok=True)

        self.retrieve_servers()
        self.retrieve_services()

    def retrieve_servers(self):
        """
        Retrieve the application servers (not the database readers)
        """

        if self.site == 'BLDR':
            site = 'bldr'
        else:
            # NOT cprk!!!
            site = 'lnx'

        if self.project == 'nowcoast':
            project = 'nc'
        else:
            project = ''

        pattern = f"{site}-{project}gisapp"

        cursor = self.conn.cursor()

        sql = f"""
               SELECT DISTINCT hostname FROM servers
               WHERE hostname like '%{pattern}%'
               """

        cursor.execute(sql)

        self.servers = [item[0] for item in cursor.fetchall()]

    def retrieve_services(self):

        if self.site == 'BLDR':
            site = 'bldr'
        else:
            # NOT cprk!!!
            site = 'lnx'

        if self.project == 'nowcoast':
            project = 'nc'
        else:
            project = ''

        pattern = f"{site}-{project}gisapp"

        cursor = self.conn.cursor()

        self.folders = []
        self.services = []

        sql = f"""
               SELECT DISTINCT b.folder, b.service
               FROM servers a INNER JOIN services b
               ON a.id = b.server_id
               WHERE hostname like '%{pattern}%'
                   AND b.priority <= {self.priority}
               """
        cursor.execute(sql)

        self.cursor.execute(sql)
        resultset = self.cursor.fetchall()
        self.folders, self.services = zip(*resultset)

    def run(self):

        # We need to determine a common ylim for a service across all servers.
        # This requires us to have the server loop on the inside rather than
        # the outside.  This requires us to create the output HTML pages up
        # front.
        doc = {}
        for server in self.servers:

            # Make the output HTML document that houses the graphs/images.
            html = etree.Element('html')
            body = etree.SubElement(html, 'body')
            h1 = etree.SubElement(body, 'h1')
            last_updated = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            h1.text = f"Last Updated:  {last_updated}"

            # Make a table of contents.
            etree.SubElement(body, 'ul')

            doc[server] = html

        for folder, service in sorted(zip(self.folders, self.services)):
            print(folder, service)

            df = self.acquire_service_data(service)
            if df.shape[0] == 0:
                print("nothing man...")
                continue

            # Figure out what the maximum number of transactions amongst the
            # servers
            transaction_max_values = []
            for server in df.hostname.unique():
                delta = df[df.hostname == server]['transactions'].diff()
                transaction_max_values.append(delta.max())

            ylims = [0, max(transaction_max_values)]

            for server in self.servers:

                server_df = df[df.hostname == server].copy()
                if server_df.shape[0] == 0:
                    print(f'no data for {server}, continuing...')
                    continue

                path = self.make_area_graph(server, service, server_df, ylims)

                # make the HTML for the table of contents section
                ul = doc[server].xpath('//ul')[0]
                li = etree.SubElement(ul, 'li')
                a = etree.SubElement(li, 'a')
                a.attrib['href'] = '#' + path.stem
                a.text = folder + '/' + path.stem

                # make the HTML for the image
                body = doc[server].xpath('//body')[0]
                div = etree.SubElement(body, 'div')
                a = etree.SubElement(div, 'a')
                a.attrib['name'] = path.stem
                img = etree.SubElement(div, 'img')
                img.attrib['src'] = path.stem + path.suffix

        # write all the output HTML files
        for server in self.servers:

            print(server)
            path = self.root / server / str(self.num_hours)
            path.mkdir(parents=True, exist_ok=True)

            path = path / 'index.html'

            tree = etree.ElementTree(doc[server])
            tree.write(str(path), pretty_print=True)

    def acquire_service_data(self, service):
        """
        Collect data for the last day.  Much more than that results in a plot
        that is too heavy.
        """

        if self.site == 'BLDR':
            site = 'bldr'
        else:
            # NOT cprk!!!
            site = 'lnx'

        if self.project == 'nowcoast':
            project = 'nc'
        else:
            project = ''

        pattern = f"{site}-{project}gisapp"

        current_time = dt.datetime.now() - dt.timedelta(hours=self.num_hours)
        sql = f"""
               SELECT
                   servers.hostname,
                   stats.time, stats.notCreated, stats.free, stats.busy,
                   stats.transactions
               FROM servers inner join services
               ON servers.id = services.server_id
               INNER JOIN statistics stats
               ON servers.id = stats.server AND services.id = stats.service
               WHERE servers.hostname like '%{pattern}%'
                 AND services.service = '{service}'
                 AND stats.time > '{current_time}'
               ORDER BY servers.hostname, stats.time ASC
               """
        logging.info(sql)
        df = pd.io.sql.read_sql(sql, self.conn)

        df['time'] = pd.to_datetime(df['time'])

        # Sometimes the statistics are negative.  Why?  MPL doesn't like it
        # in area plots.
        df = df.query('busy >= 0 & free >= 0 & notCreated >= 0')

        return df

    def make_area_graph(self, server, service, df, ylims):
        """
        Return None if the plot attempt was unsuccessful.
        """
        df.set_index('time', inplace=True)

        print(f"NOBS for {server}/{service} is {df.shape[0]}")

        fig, ax1 = plt.subplots()
        df[['busy', 'free', 'notCreated']].plot.area(ax=ax1)

        ax1.set_title(service)
        ax1.set_ylabel('instances')

        ax2 = ax1.twinx()

        delta = df['transactions'].diff()
        delta[delta < 0] = np.nan
        delta.plot(ax=ax2, color='black', linewidth=1)

        ax2.set_ylabel('Transactions')
        ax2.set_ylim(ylims)
        ax2.grid(None)

        ax1.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%d %H:%M'))
        legend = ax1.get_legend()
        frame = legend.get_frame()
        frame.set_facecolor('white')
        frame.set_edgecolor('black')

        fig.tight_layout()

        parent_dir = self.root / server / str(self.num_hours)
        parent_dir.mkdir(parents=True, exist_ok=True)
        path = parent_dir / f"{service}.png"
        print(f"Writing to {path}")
        fig.savefig(str(path))
        return path
