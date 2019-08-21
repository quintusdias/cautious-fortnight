# Standard library imports
import datetime as dt

# 3rd party library imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor


class IPAddressProcessor(CommonProcessor):
    """
    Attributes
    ----------
    time_series_sql : str
        SQL to collect a coherent timeseries of folder/service information.
    """

    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(project, **kwargs)

        self.time_series_sql = """
            SELECT a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes, b.ip_address
            FROM ip_address_logs a
            INNER JOIN known_ip_addresses b
            ON a.id = b.id
            GROUP BY a.date, b.ip_address
            ORDER BY a.date
            """

        self.data_retention_days = 7

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        IP addresses.
        """
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
                  AND name LIKE '%ip_address%'
              """
        df = pd.read_sql(sql, self.conn)
        if len(df) == 2:
            # We're good.
            return

        cursor = self.conn.cursor()

        # Create the known IP addresses table.  The IP addresses must be
        # unique.
        sql = """
              CREATE TABLE known_ip_addresses (
                  id integer PRIMARY KEY,
                  ip_address text,
                  name text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_ip_address
              ON known_ip_addresses(ip_address)
              """
        cursor.execute(sql)

        # Create the IP address logs table.
        sql = """
              CREATE TABLE ip_address_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  CONSTRAINT fk_known_ip_address_id
                      FOREIGN KEY (id)
                      REFERENCES known_ip_addresses(id)
                      ON DELETE CASCADE
              )
              """
        cursor.execute(sql)

        # Unfortunately the index cannot be unique here.
        sql = """
              CREATE INDEX idx_ip_address_logs_date
              ON ip_address_logs(date)
              """
        cursor.execute(sql)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        columns = ['date', 'ip_address', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Aggregate by the set frequency and referer, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'ip_address']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        df = self.replace_ip_addresses_with_ids(df)

        df = self.merge_with_database(df, 'ip_address_logs')

        df.to_sql('ip_address_logs', self.conn,
                  if_exists='append', index=False)

        self.conn.commit()

        self.records = []

    def replace_ip_addresses_with_ids(self, df_orig):
        """
        The IP addresses themselves are not to be logged.  Rather, we wish to
        log an ID standing for the IP address.
        """

        sql = """
              SELECT id, ip_address from known_ip_addresses
              """
        known_ips = pd.read_sql(sql, self.conn)

        # Get the referer IDs
        df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        # How many IP addresses have NaN for IDs?  This must populate the known
        # IP address table before going further.
        unknown_ips = df['ip_address'][df['id'].isnull()].unique()
        if len(unknown_ips) > 0:
            new_ips_df = pd.Series(unknown_ips, name='ip_address').to_frame()

            new_ips_df.to_sql('known_ip_addresses', self.conn,
                              if_exists='append', index=False)

            sql = """
                  SELECT id, ip_address from known_ip_addresses
                  """
            known_ips = pd.read_sql(sql, self.conn)

            df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        df = df.drop(['ip_address'], axis='columns')
        return df

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the IP addresses.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        self.get_timeseries()

        df = self.df_today.copy().groupby('ip_address').sum()

        # Find the top 5 by hits over the past week, plus the top 5 by nbytes.
        top5_hits = df.sort_values(by='hits', ascending=False) \
                      .head(5) \
                      .index \
                      .values \
                      .tolist()
        top5_nbytes = df.sort_values(by='nbytes', ascending=False) \
                        .head(5) \
                        .index \
                        .values \
                        .tolist()
        top_ips = set(top5_hits + top5_nbytes)

        self.summarize_ip_addresses(top_ips, html_doc)
        self.summarize_transactions(top_ips, html_doc)
        self.summarize_bandwidth(top_ips, html_doc)

    def summarize_transactions(self, top_ips, html_doc):

        df = self.df[self.df['ip_address'].isin(top_ips)].copy()

        # Rescale from hits/hour to hits/seconds.
        df['hits'] /= 3600

        df = df.pivot(index='date', columns='ip_address', values='hits')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'Top IPs:  Hits per Second',
            'filename': 'top_ip_hits.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_bandwidth(self, top_ips, html_doc):
        """
        Create plot of bandwidth usage of top IP addresses.

        Parameters
        ----------
        top_ips : list
            IP addresses with the highest bandwidth.
        html_doc : etree Element
            The plot image is to be inserted into this document.
        """
        df = self.df[self.df['ip_address'].isin(top_ips)].copy()
        df['nbytes'] /= (1024 * 1024)
        df = df.pivot(index='date', columns='ip_address', values='nbytes')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'Top IPs:  MBytes per Hour',
            'filename': 'top_ip_nbytes.png'
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_ip_addresses(self, top_ips, html_doc):
        df = self.df_today.copy().groupby('ip_address').sum()

        total_hits = df['hits'].sum()
        total_bytes = df['nbytes'].sum()
        total_errors = df['errors'].sum()

        df['hits %'] = df['hits'] / total_hits * 100
        df['GBytes'] = df['nbytes'] / (1024 ** 3)  # GBytes
        df['GBytes %'] = df['nbytes'] / total_bytes * 100

        idx = df['errors'].isnull()
        df.loc[idx, ('errors')] = 0

        df['errors: % of all hits'] = df['errors'] / total_hits * 100
        df['errors: % of all errors'] = df['errors'] / total_errors * 100

        # How to these top 10 make up today's traffic?
        df = df[df.index.isin(top_ips)].sort_values(by='hits', ascending=False)

        # Reorder the columns
        reordered_cols = [
            'hits',
            'hits %',
            'GBytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]

        df = df.sort_values(by='hits', ascending=False)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'iptable',
            'atext': 'Top IPs Table',
            'h1text': f'Top IP Addresses by Hits: {yesterday}',
        }
        self.create_html_table(df, html_doc, **kwargs)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.

        If it's Monday, just drop the tables.
        """
        cursor = self.conn.cursor()

        if dt.date.today().weekday() != 0:
            # If it's not Monday, do nothing.
            return

        # Ok, it's Monday, drop the IP address tables, they will be recreated.
        sql = """
              DROP INDEX idx_ip_address_logs_date
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP TABLE ip_address_logs
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP INDEX idx_ip_address
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP TABLE known_ip_addresses
              """
        self.logger.info(sql)
        cursor.execute(sql)

        self.conn.commit()
