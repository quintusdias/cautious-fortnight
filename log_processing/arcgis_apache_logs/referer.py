#!/usr/bin/env python

# Standard library imports
import datetime as dt
import urllib.parse

# 3rd party library imports
from matplotlib.ticker import FuncFormatter
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

    def process_match(self, apache_match):
        """
        What referers were given?
        """
        timestamp = apache_match.group('timestamp')
        referer = apache_match.group('referer')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        self.records.append((timestamp, referer, 1, error, nbytes))
        if len(self.records) == self.MAX_RAW_RECORDS:
            self.process_records()

    def flush(self):
        self.process_records()

    def process_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        columns = ['date', 'referer', 'hits', 'errors', 'nbytes']
        df = pd.DataFrame(self.records, columns=columns)

        # Convert the apache timestamp into a python timestamp.  Make the
        # number of bytes numeric.
        format = '%d/%b/%Y:%H:%M:%S %z'
        df['date'] = pd.to_datetime(df['date'], format=format)
        df['nbytes'] = df['nbytes'].astype(int)

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
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df = self.replace_referers_with_ids(df)

        # Ok, suitable to send to the database now.
        msg = f'Logging {len(df)} referer records to database.'
        self.logger.info(msg)

        df.to_sql('referer_logs', self.conn, if_exists='append', index=False)
        self.conn.commit()

        # Reset
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

            msg = f'Logging {len(new_df)} new referer records.'
            self.logger.info(msg)

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

    def process_graphics(self, html_doc):
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

        kwargs = {
            'title': 'GBytes per Hour',
            'imagefile': 'referers_bytes.png',
        }
        self.create_bandwidth_output(df, html_doc, **kwargs)

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

        df = df.pivot(index='date', columns='referer', values='hits')

        kwargs = {
            'title': 'Hits per Hour (not including errors)',
            'filename': 'referers_hits.png',
            'yaxis_formatter': FuncFormatter(millions_fcn),
        }
        self.create_transactions_output(df, html_doc, **kwargs)

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
            'GBytes',
            'GBytes %',
            'hits',
            'hits %',
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
