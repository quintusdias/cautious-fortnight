#!/usr/bin/env python

# Standard library imports
import datetime as dt
import urllib.parse

# 3rd party library imports
import numpy as np
import pandas as pd
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'


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
    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        known_referers : dataframe
            All referers that have been previously encountered.
        """
        super().__init__(project, **kwargs)

        self.time_series_sql = """
            SELECT a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes, b.name as referer
            FROM referer_logs a
            INNER JOIN known_referers b
            ON a.id = b.id
            GROUP BY a.date, referer
            ORDER BY a.date
            """

        self.data_retention_days = 7

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the referers.
        """

        cursor = self.conn.cursor()

        # Do the referer tables exist?
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
              """
        df = pd.read_sql(sql, self.conn)

        if 'known_referers' not in df.name.values:

            sql = """
                  CREATE TABLE known_referers (
                      id integer PRIMARY KEY,
                      name text
                  )
                  """
            cursor.execute(sql)
            sql = """
                  CREATE UNIQUE INDEX idx_referer
                  ON known_referers(name)
                  """
            cursor.execute(sql)

        if 'referer_logs' not in df.name.values:

            sql = """
                  CREATE TABLE referer_logs (
                      date integer,
                      id integer,
                      hits integer,
                      errors integer,
                      nbytes integer,
                      CONSTRAINT fk_known_referers_id
                          FOREIGN KEY (id)
                          REFERENCES known_referers(id)
                          ON DELETE CASCADE
                  )
                  """
            cursor.execute(sql)

            # Unfortunately the index cannot be unique here.
            sql = """
                  CREATE UNIQUE INDEX idx_referer_logs_date
                  ON referer_logs(date, id)
                  """
            cursor.execute(sql)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
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

        # Remake the date into a single column, a timestamp
        df_ref['date'] = df_ref['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df_ref = self.replace_referers_with_ids(df_ref)

        df_ref = self.merge_with_database(df_ref, 'referer_logs')

        df_ref.to_sql('referer_logs', self.conn,
                      if_exists='append', index=False)
        self.conn.commit()

        # As a last step, aggregate the data without regard to the referer.
        df_summary = (df.set_index('date')
                        .resample(self.frequency)
                        .sum()
                        .reset_index())

        # Remake the date into a single column, a timestamp
        df_summary['date'] = df_summary['date'].astype(np.int64) // 1e9

        df_summary = self.merge_with_database(df_summary, 'summary')

        df_summary.to_sql('summary', self.conn,
                          if_exists='append', index=False)
        self.conn.commit()

        # Reset for the next round of records.
        self.records = []

    def replace_referers_with_ids(self, df_orig):
        """
        Don't log the actual referer names to the database, log the ID instead.
        """

        sql = """
              SELECT * from known_referers
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

            new_df.to_sql('known_referers', self.conn,
                          if_exists='append', index=False)

            sql = """
                  SELECT * from known_referers
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
        sql = """
              DELETE FROM referer_logs WHERE date < ?
              """
        datenum = (
            dt.datetime.now()
            - dt.timedelta(days=self.data_retention_days)
        ).timestamp()

        cursor = self.conn.cursor()
        cursor.execute(sql, (datenum,))
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
