#!/usr/bin/env python

# Standard library imports
import datetime as dt
import urllib.parse

# 3rd party library imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


class RefererProcessor(CommonProcessor):
    """
    Attributes
    ----------
    conn : obj
        database connectivity
    database : path or str
        Path to database
    project : str
        Either nowcoast or idpgis
    time_series_sql : str
        SQL to collect a coherent timeseries of folder/service information.
    """
    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        known_referers : dataframe
            All referers that have been previously encountered.
        """
        super().__init__(**kwargs)

        self.time_series_sql = f"""
            SELECT a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes, b.name as referer
            FROM referer_logs a INNER JOIN referer_lut b
            ON a.id = b.id
            GROUP BY a.date, referer
            ORDER BY a.date
            """

        self.data_retention_days = 7

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.logger.info(f'Referers:  processing {len(df)} records...')
        columns = ['date', 'referer', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Throw away any the query string in the referer.
        def fcn(referer):
            p = urllib.parse.urlparse(referer)
            if p.query == '':
                # No query string, use the referer as-is.
                pass
            else:
                referer = f"{p.scheme}://{p.netloc}{p.path}"
            return referer

        df['referer'] = df['referer'].apply(fcn)

        # Aggregate by the set frequency and referer, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'referer']
        df_ref = df.set_index('date').groupby(groupers).sum().reset_index()

        # Have to have the same column names as the database.
        df_ref = self.replace_referers_with_ids(df_ref)

        df_ref = self.merge_with_database(df_ref, 'referer_logs')

        self.to_table(df_ref, 'referer_logs')

        # Reset for the next round of records.
        self.records = []

        self.logger.info('Referers:  done processing records...')

    def replace_referers_with_ids(self, df_orig):
        """
        Don't log the actual referer names to the database, log the ID instead.
        """

        sql = f"""
              SELECT * from referer_lut
              """
        known_referers = pd.read_sql(sql, self.conn)

        # Get the referer IDs
        df = pd.merge(df_orig, known_referers,
                      how='left', left_on='referer', right_on='name')

        # How many referers have NaN for IDs?  This must populate the known
        # referers table before going further.
        unknown_referers = df['referer'][df['id'].isnull()].unique()
        if len(unknown_referers) > 0:
            new_df = pd.Series(unknown_referers, name='name').to_frame()

            self.to_table(new_df, 'referer_lut')

            sql = f"""
                  SELECT * from referer_lut
                  """
            known_referers = pd.read_sql(sql, self.conn)
            df = pd.merge(df_orig, known_referers,
                          how='left', left_on='referer', right_on='name')

        df.drop(['referer', 'name'], axis='columns', inplace=True)

        return df

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.

        Delete anything older than 7 days.
        """
        if dt.date.today().weekday() != 0:
            # If it's not Monday, do nothing.
            return

        # Ok, it's Monday, drop the IP address tables, they will be recreated.
        cursor = self.conn.cursor()

        sql = """
              delete from referer_logs
              """
        self.logger.info(sql)
        cursor.execute(sql)

        self.conn.commit()

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the referers.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        self.get_timeseries()
        self.summarize_referers(html_doc)
        self.summarize_transactions(html_doc)
        self.summarize_bandwidth(html_doc)

    def get_top_referers(self):
        # who are the top referers for today?
        df = self.df_today.copy()

        df['valid_hits'] = df['hits'] - df['errors']
        top_referers = (df.groupby('referer')
                          .sum()
                          .sort_values(by='valid_hits', ascending=False)
                          .head(n=7)
                          .index)

        return top_referers

    def summarize_bandwidth(self, html_doc):
        """
        Create a PNG showing the top referers (bytes) over the last few days.
        """
        top_referers = self.get_top_referers()

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df = self.df[self.df.referer.isin(top_referers)].sort_values(by='date')
        df = df[['date', 'referer', 'nbytes']]
        df['nbytes'] /= (1024 ** 3)

        df = df.pivot(index='date', columns='referer', values='nbytes')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'GBytes per Hour',
            'filename': f'{self.project}_referers_bytes.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the top referers over the last few days.
        """
        top_referers = self.get_top_referers()

        df = self.df.copy()

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df = df[df.referer.isin(top_referers)].sort_values(by='date').copy()
        df['hits'] = df['hits'] - df['errors']
        df = df[['date', 'referer', 'hits']]

        # Rescale them from hits/hour to hits/second
        df['hits'] /= 3600

        df = df.pivot(index='date', columns='referer', values='hits')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': (
                'Hits per Second (averaged per hour, not including errors)'
            ),
            'filename': f'{self.project}_referers_hits.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_referers(self, html_doc):
        """
        Calculate

              I) percentage of hits for each referer
             II) percentage of hits for each referer that are 403s
            III) percentage of total 403s for each referer

        Just for the latest day, though.
        """
        df = self.df_today.copy().groupby('referer').sum()

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
            'hits',
            'hits %',
            'GBytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]
        df = df.sort_values(by='hits', ascending=False).head(15)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'referers',
            'atext': 'Top Referers',
            'h1text': f'Top Referers by Hits: {yesterday}'
        }
        self.create_html_table(df, html_doc, **kwargs)
