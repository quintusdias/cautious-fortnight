# Standard library imports
import datetime as dt

# 3rd party library imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2.extras

# Local imports
from .common import CommonProcessor


class IPAddressProcessor(CommonProcessor):
    """
    Attributes
    ----------
    time_series_sql : str
        SQL to collect a coherent timeseries of folder/service information.
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(**kwargs)

        self.time_series_sql = f"""
            SELECT a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes, b.ip_address
            FROM ip_address_logs a
            INNER JOIN ip_address_lut b
            ON a.id = b.id
            GROUP BY a.date, b.ip_address
            ORDER BY a.date
            """

        self.data_retention_days = 7

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.logger.info(f'IP addresses:  processing {len(df)} records...')
        columns = ['date', 'ip_address', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Aggregate by the set frequency and referer, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'ip_address']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        df = self.replace_ip_addresses_with_ids(df)

        df = self.merge_with_database(df, 'ip_address_logs')

        self.to_table(df, 'ip_address_logs')

        self.records = []
        self.logger.info('IP addresses:  done processing records...')

    def replace_ip_addresses_with_ids(self, df_orig):
        """
        The IP addresses themselves are not to be logged.  Rather, we wish to
        log the IDs standing in for the IP address.
        """
        self.logger.info('about to update the IP address LUT...')

        sql = f"""
              SELECT id, ip_address from ip_address_lut
              """
        known_ips = pd.read_sql(sql, self.conn)

        # match known IP addresses with the current dataset
        df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        # How many IP addresses have NaN for IDs?  This must populate the IP
        # address lookup table before going further.
        unknown_ips = df['ip_address'][df['id'].isnull()].unique()
        if len(unknown_ips) > 0:
            new_ips_df = pd.Series(unknown_ips, name='ip_address').to_frame()

            self.to_table(new_ips_df, 'ip_address_lut')

            sql = f"""
                  SELECT id, ip_address from ip_address_lut
                  """
            known_ips = pd.read_sql(sql, self.conn)

            df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        df = df.drop(['ip_address'], axis='columns')
        self.logger.info('finished updating the IP address LUT...')
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

        # Ok, it's Monday, drop the IP address tables
        sql = """
              delete from ip_address_logs
              """
        self.logger.info(sql)
        cursor.execute(sql)

        self.conn.commit()
