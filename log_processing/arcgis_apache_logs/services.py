# Standard library imports
import datetime as dt
import re

# 3rd party libraries
from lxml import etree
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor


class ServicesProcessor(CommonProcessor):
    """
    Attributes
    ----------
    regex : object
        Parses arcgis folders, services, types from the request path.
    time_series_sql : str
        SQL to collect a coherent timeseries of folder/service information.
    """
    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(project, **kwargs)

        pattern = r'''
                   /(nowcoast|idpgis).ncep.noaa.gov.akadns.net
                   /arcgis
                   (?P<rest>/rest)?
                   /services
                   /(?P<folder>\w+)
                   /(?P<service>\w+)
                   /(?P<service_type>\w+)
                   (
                       (
                           # an export map draw
                           (?P<export>/(export|exportImage))
                           .+?
                           (?P<imageformat>f=image)
                       )
                       |
                       (
                           # a WMS map draw
                           (?P<wmsserver>/wmsserver)
                           .+?
                           (?P<wmsgetmap>request=getmap)
                       )
                   )?
                   '''
        self.regex = re.compile(pattern, re.VERBOSE | re.IGNORECASE)

        self.time_series_sql = """
            SELECT
                a.date,
                SUM(a.hits) as hits,
                SUM(a.errors) as errors,
                SUM(a.nbytes) as nbytes,
                SUM(a.export_mapdraws) as export_mapdraws,
                SUM(a.wms_mapdraws) as wms_mapdraws,
                b.folder, b.service, b.service_type
            FROM service_logs a
            INNER JOIN known_services b
            ON a.id = b.id
            GROUP BY a.date, b.folder, b.service, b.service_type
            ORDER BY a.date
            """
        self.records = []

        self.data_retention_days = 30

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the services.
        """
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
                  AND name LIKE '%service%'
              """
        df = pd.read_sql(sql, self.conn)
        if len(df) == 2:
            # We're good.
            return

        cursor = self.conn.cursor()

        # Create the known services and logs tables.
        sql = """
              CREATE TABLE known_services (
                  id integer PRIMARY KEY,
                  folder text,
                  service text,
                  service_type text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_services
              ON known_services(folder, service, service_type)
              """
        cursor.execute(sql)

        sql = """
              CREATE TABLE service_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  export_mapdraws integer,
                  wms_mapdraws integer,
                  CONSTRAINT fk_known_services_id
                      FOREIGN KEY (id)
                      REFERENCES known_services(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)

        sql = """
              CREATE UNIQUE INDEX idx_services_logs_date
              ON service_logs(date, id)
              """
        cursor.execute(sql)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        columns = ['date', 'path', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        df_svc = df['path'].str.extract(self.regex)

        # Cleanly determine export and WMS map draws.
        df_svc['export_mapdraws'] = (~df_svc['export'].isnull()).astype(int)
        df_svc['wms_mapdraws'] = (~df_svc['wmsgetmap'].isnull()).astype(int)

        cols = [
            'folder', 'service', 'service_type', 'export_mapdraws',
            'wms_mapdraws'
        ]
        df = pd.concat((df, df_svc[cols]), axis='columns')
        df = df.drop('path', axis='columns')

        # Aggregate by the set frequency and service, taking sums.
        groupers = [
            pd.Grouper(freq=self.frequency),
            'folder', 'service', 'service_type'
        ]
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df = self.replace_folders_and_services_with_ids(df)
        if len(df) == 0:
            return

        df = self.merge_with_database(df, 'service_logs')

        df.to_sql('service_logs', self.conn, if_exists='append', index=False)
        self.conn.commit()

        # Reset
        self.records = []

    def replace_folders_and_services_with_ids(self, df_orig):

        sql = """
              SELECT * from known_services
              """
        known_services = pd.read_sql(sql, self.conn)

        group_cols = ['folder', 'service', 'service_type']

        # Get the service IDs
        df = pd.merge(df_orig, known_services,
                      how='left',
                      left_on=group_cols,
                      right_on=group_cols)

        # How many services have NaN for IDs?  This must be dropped.
        dfnull = df[df.id.isnull()]
        n = len(dfnull)
        msg = f"Dropping {n} unmatched IDs"
        self.logger.info(msg)
        df = df.dropna(subset=['id'])

        # We have the service ID, we don't need the folder, service, or
        # service_type columns anymore.
        df = df.drop(group_cols, axis='columns')

        return df

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the services.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        self.get_timeseries()
        self.create_services_table(html_doc)

        # Link in a folder list.
        toc = html_doc.xpath('body/ul[@class="tableofcontents"]')[0]
        li = etree.SubElement(toc, 'li')
        li.text = 'Folders'

        ul = etree.Element('ul', id='services')
        li[:] = [ul]

        self.summarize_transactions(html_doc)

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the services over the last few days.
        """
        folders = self.df_today.folder.unique()

        for folder in folders:

            # Now restrict the hourly data over the last few days to those
            # referers.  Then restrict to valid hits.  And rename valid_hits to
            # hits.
            df = self.df[self.df.folder == folder].copy()

            df['hits'] = df['hits'] - df['errors']
            df = df[['date', 'service', 'service_type', 'hits']]

            # If there are services with overlapping names, e.g. CO_OPS
            # mapserver and featureservers, combine the service and
            # service_type columns.  Otherwise drop the service_type column.
            dfg = df.groupby(['service', 'service_type']).count()
            if not np.all(np.diff(dfg.index.codes[0]) > 0):
                # Overlapping names, so collapse the service and service_type
                # columns.
                df['service'] = df['service'] + '/' + df['service_type']
            df = df[['date', 'service', 'hits']]

            df = df.pivot(index='date', columns='service', values='hits')

            # Order them by max value.
            s = df.max().sort_values(ascending=False)
            df = df[s.index]

            service_max = df.max()

            # Drop any services where the total hits too low.
            df = df.drop(service_max[service_max <= 1].index.values, axis=1)
            if df.shape[1] == 0:
                continue

            if df.max().max() > 3600:
                # Rescale to from hits/hour to hits/second
                df /= 3600
                title = f'{folder} folder:  Hits per second'
            else:
                title = f'{folder} folder:  Hits per hour'

            fig, ax = plt.subplots(figsize=(15, 7))
            df.plot(ax=ax)

            kwargs = {
                'title': title,
                'filename': f'{folder}_hits.png',
                'folder': folder,
            }
            self.write_html_and_image_output(df, html_doc, **kwargs)

    def create_services_table(self, html_doc):
        """
        Calculate

              I) percentage of hits for each service
             II) percentage of errors for each service

        Just for the latest day, though.
        """
        df = self.df_today.copy().groupby(['service', 'service_type']).sum()

        total_hits = df['hits'].sum()
        total_bytes = df['nbytes'].sum()
        total_errors = df['errors'].sum()
        df['mapdraws'] = df['export_mapdraws'] + df['wms_mapdraws']

        df = df[['hits', 'nbytes', 'errors', 'mapdraws']].copy()
        df['hits %'] = df['hits'] / total_hits * 100
        df['mapdraw %'] = df['mapdraws'] / df['hits'] * 100
        df['GBytes'] = df['nbytes'] / (1024 ** 3)  # GBytes
        df['GBytes %'] = df['nbytes'] / total_bytes * 100

        df['errors: % of all hits'] = df['errors'] / total_hits * 100
        df['errors: % of all errors'] = df['errors'] / total_errors * 100

        # Reorder the columns
        reordered_cols = [
            'hits',
            'hits %',
            'mapdraw %',
            'GBytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors',
        ]
        df = df[reordered_cols]

        df = df.sort_values(by='hits', ascending=False)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()

        ptext = (
            "\"hits %\" is the ratio of service hits to the total number of "
            "hits, so this column should add to 100.  \"mapdraw %\" is the "
            "ratio of service mapdraws to the service hits."
        )
        kwargs = {
            'aname': 'servicetable',
            'atext': 'Services Table',
            'h1text': f'Services by Hits: {yesterday}',
            'ptext': ptext,
        }
        self.create_html_table(df, html_doc, **kwargs)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.

        Delete anything older than 30 days.
        """
        sql = """
              DELETE FROM service_logs WHERE date < ?
              """
        datenum = (
            dt.datetime.now() - dt.timedelta(days=self.data_retention_days)
        ).timestamp()

        cursor = self.conn.cursor()
        cursor.execute(sql, (datenum,))
        self.conn.commit()
