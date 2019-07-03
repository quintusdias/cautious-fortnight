# Standard library imports
import datetime as dt
import re

# 3rd party libraries
from lxml import etree
from matplotlib.ticker import FuncFormatter, NullFormatter
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor, thousands_fcn, millions_fcn


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
                   (/rest)?
                   /services
                   /(?P<folder>\w+)
                   /(?P<service>\w+)
                   /.*
                   '''
        self.regex = re.compile(pattern, re.VERBOSE)

        self.time_series_sql = """
            SELECT
                a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                SUM(a.nbytes) as nbytes, b.folder, b.service
            FROM service_logs a
            INNER JOIN known_services b
            ON a.id = b.id
            GROUP BY a.date, b.folder, b.service
            ORDER BY a.date
            """
        self.records = []

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
                  service text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_services
              ON known_services(folder, service)
              """
        cursor.execute(sql)

        sql = """
              CREATE TABLE service_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  FOREIGN KEY (id) REFERENCES known_services(id)
              )
              """
        cursor.execute(sql)

        # Unfortunately the index cannot be unique here.
        sql = """
              CREATE INDEX idx_services_logs_date
              ON service_logs(date)
              """
        cursor.execute(sql)

    def process_match(self, apache_match):
        """
        What services were hit?
        """
        timestamp = apache_match.group('timestamp')
        path = apache_match.group('path')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        m = self.regex.match(path)
        if m is None:
            return

        folder = m.group('folder')
        service = m.group('service')

        record = (timestamp, folder, service, 1, error, nbytes)
        self.records.append(record)
        if len(self.records) == self.MAX_RAW_RECORDS:
            self.process_raw_records()

    def flush(self):
        self.process_raw_records()

    def process_raw_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        columns = [
            'date', 'folder', 'service', 'hits', 'errors', 'nbytes'
        ]
        df = pd.DataFrame(self.records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['date'] = pd.to_datetime(df['date'], format=format)

        df['nbytes'] = df['nbytes'].astype(int)

        # Aggregate by the set frequency and service, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'folder', 'service']
        df = df.set_index('date').groupby(groupers).sum()

        df = df.reset_index()

        df['date'] = df['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df = self.replace_folders_and_services_with_ids(df)

        msg = f'Logging {len(df)} service records to database.'
        self.logger.info(msg)

        df.to_sql('service_logs', self.conn, if_exists='append', index=False)
        self.conn.commit()

        # Reset
        self.records = []

    def replace_folders_and_services_with_ids(self, df_orig):

        sql = """
              SELECT * from known_services
              """
        known_services = pd.read_sql(sql, self.conn)

        # Get the service IDs
        df = pd.merge(df_orig, known_services,
                      how='left',
                      left_on=['folder', 'service'],
                      right_on=['folder', 'service'])

        # How many services have NaN for IDs?  This must populate the known
        # referers table before going further.
        new = df[['folder', 'service']][df['id'].isnull()]
        new = new.groupby(['folder', 'service']).count().reset_index()

        if len(new) > 0:

            msg = f'Logging {len(new)} service records.'
            self.logger.info(msg)

            new.to_sql('known_services', self.conn,
                       if_exists='append', index=False)

            sql = """
                  SELECT * from known_services
                  """
            known_services = pd.read_sql(sql, self.conn)
            df = pd.merge(df_orig, known_services,
                          how='left',
                          left_on=['folder', 'service'],
                          right_on=['folder', 'service'])

        df.drop(['folder', 'service'], axis='columns', inplace=True)

        return df

    def process_graphics(self, html_doc):
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
            df = df[['date', 'service', 'hits']]

            df = df.pivot(index='date', columns='service', values='hits')

            # Set the plot order according to the max values of the services.
            df = df[df.max().sort_values().index.values]

            service_max = df.max()
            folder_max = service_max.max()

            # Drop any services where the total hits are zero.
            df = df.drop(service_max[service_max == 0].index.values, axis=1)

            if folder_max <= 1:
                continue
            elif folder_max < 1000:
                formatter = NullFormatter()
            elif folder_max < 1000000:
                formatter = FuncFormatter(thousands_fcn)
            else:
                formatter = FuncFormatter(millions_fcn)

            kwargs = {
                'title': f'{folder} folder:  Hits per Hour',
                'filename': f'{folder}_hits.png',
                'yaxis_formatter': formatter,
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
        df = self.df_today.copy().groupby('service').sum()

        total_hits = df['hits'].sum()
        total_bytes = df['nbytes'].sum()
        total_errors = df['errors'].sum()

        df = df[['hits', 'nbytes', 'errors']].copy()
        df['hits %'] = df['hits'] / total_hits * 100
        df['GBytes'] = df['nbytes'] / (1024 ** 3)  # GBytes
        df['GBytes %'] = df['nbytes'] / total_bytes * 100

        idx = df['errors'].isnull()
        df.loc[idx, ('errors')] = 0
        df['errors'] = df['errors'].astype(np.uint64)

        df['errors: % of all hits'] = df['errors'] / total_hits * 100
        df['errors: % of all errors'] = df['errors'] / total_errors * 100

        # Reorder the columns
        reordered_cols = [
            'GBytes',
            'GBytes %',
            'hits',
            'hits %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]

        df = df.sort_values(by='hits', ascending=False)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'servicetable',
            'atext': 'Services Table',
            'h1text': f'Services by Hits: {yesterday}',
        }
        self.create_html_table(df, html_doc, **kwargs)
