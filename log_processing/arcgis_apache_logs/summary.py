#!/usr/bin/env python

# Standard library imports
import datetime as dt

# 3rd party library imports
import lxml.etree
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


class SummaryProcessor(CommonProcessor):
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
        """
        super().__init__(project, **kwargs)

        self.time_series_sql = """
            SELECT
                date,
                SUM(hits) as hits,
                SUM(errors) as errors,
                SUM(nbytes) as nbytes,
                SUM(mapdraws) as mapdraws
            FROM summary
            GROUP BY date
            ORDER BY date
            """

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the summary.
        """

        cursor = self.conn.cursor()

        # Do the summary tables exist?
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
              """
        df = pd.read_sql(sql, self.conn)

        if 'summary' not in df.name.values:

            sql = """
                  CREATE TABLE summary (
                      date integer,
                      hits integer,
                      mapdraws integer,
                      errors integer,
                      nbytes integer
                  )
                  """
            cursor.execute(sql)

            sql = """
                  CREATE UNIQUE INDEX idx_summary_date
                  ON summary(date)
                  """
            cursor.execute(sql)

        if "burst_summary" not in df.name.values:

            sql = """
                  CREATE TABLE burst_summary (
                      date integer,
                      hits integer,
                      errors integer,
                      nbytes integer
                  )
                  """
            cursor.execute(sql)

            sql = """
                  CREATE UNIQUE INDEX idx_burst_summary_date
                  ON summary(date)
                  """
            cursor.execute(sql)

        if "burst_staging" not in df.name.values:

            sql = """
                  CREATE TABLE burst_staging (
                      date integer,
                      hits integer,
                      errors integer,
                      nbytes integer
                  )
                  """
            cursor.execute(sql)

            sql = """
                  CREATE INDEX idx_burst_staging_date
                  ON summary(date)
                  """
            cursor.execute(sql)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.
        """
        # Do a daily rebuild of the database, just to try to keep things in
        # order.  It's not too expensive.
        self.conn.execute('VACUUM')
        return

        # Drop any records from the burst staging table that are older than
        # 7 days.
        sql = """
              DELETE FROM burst_staging
              WHERE date < ?
              """
        datenum = (dt.datetime.now() - dt.timedelta(days=7)).timestamp()
        self.cursor.execute(sql, (datenum,))

        self.conn.commit()

    def post_process_burst(self):
        fig, ax = plt.subplots()

        # Get the hourly data.
        # Plot the errors and mapdraws
        self.get_timeseries()

        df = self.df.copy().tail(n=24)

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.
        # hits.
        df['hits'] = df['hits']
        df = df[['date', 'hits', 'errors', 'mapdraws']]
        df = df.set_index('date')

        # Turn the data from hits/hour to hits/second
        df['hits'] /= 3600
        df['errors'] /= 3600
        df['mapdraws'] /= 3600

        df.drop('hits', axis='columns', inplace=True)

        h1 = df.plot(ax=ax)


        sql = """
              SELECT date,
                     SUM(hits) as hits,
                     SUM(errors) as errors,
                     SUM(nbytes) as nbytes
              FROM burst_staging
              GROUP BY date
              ORDER BY date
              """
        df = pd.read_sql(sql, self.conn)
        df['date'] = pd.to_datetime(df['date'], unit='s')

        df = df.set_index('date')

        # Get average rate per second.
        df['hits'] /= 60
        df['errors'] /= 60
        df['nbytes'] /= 60

        # Get the rolling mean.
        dfr = df['hits'].rolling(15).aggregate([np.mean, np.max, np.min])
        h = plot.bar(dfr.index.values, dfr['amax'] - dfr['amin'], bottom=dfr['amin'], edgecolor='none')

        #ax.fill_between(dfr.index.values, dfr['amin'], dfr['amax'], gid='fill', zorder=1)

    def process_raw_records(self, raw_df):

        columns = ['date', 'hits', 'errors', 'nbytes']
        df = raw_df[columns].copy()

        # Do the burst summary (1 minute)
        df = (df.set_index('date')
                .resample('T')
                .sum()
                .reset_index())
        df['date'] = df['date'].astype(np.int64) // 1e9
        df.to_sql('burst_staging', self.conn, if_exists='append', index=False)

        # Do the hourly summary
        df = raw_df[columns].copy()

        # As a last step, aggregate the data without regard to the referer.
        df = (df.set_index('date')
                .resample(self.frequency)
                .sum()
                .reset_index())

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        df = self.merge_with_database(df, 'summary')

        # Now merge with the map draw information from the services table.
        starting_date = df.loc[0]['date']
        sql = """
              SELECT date,
                     SUM(export_mapdraws) as export_mapdraws,
                     SUM(wms_mapdraws) as wms_mapdraws
              FROM service_logs
              WHERE date >= ?
              GROUP BY date
              """
        df_svc = pd.read_sql(sql, self.conn, params=(starting_date,))
        df = pd.merge(df, df_svc, on='date', how='left')
        df = df.fillna(value=0)

        df['mapdraws'] = df['export_mapdraws'] + df['wms_mapdraws']
        df = df.drop(['export_mapdraws', 'wms_mapdraws'], axis='columns')

        df.to_sql('summary', self.conn, if_exists='append', index=False)
        self.conn.commit()

    def process_graphics(self, html_doc):

        body = html_doc.xpath('body')[0]
        div = lxml.etree.SubElement(body, 'div')
        lxml.etree.SubElement(div, 'hr')
        h1 = lxml.etree.SubElement(div, 'h1')
        h1.text = f"{self.project.upper()} Summary"

        self.get_timeseries()
        self.summarize_transactions(html_doc)
        self.summarize_bandwidth(html_doc)

    def summarize_bandwidth(self, html_doc):
        """
        Create an image showing the bandwidth over the last few days.
        """
        df = self.df[['date', 'nbytes']].copy()
        df.columns = ['date', 'bandwidth']
        df.loc[:, 'bandwidth'] /= (1024 ** 4)
        df = df.set_index('date')

        total_throughput = df.tail(n=24).sum().values[0] * 1000

        # Downsample to days.
        df = df.resample('D').sum()

        text = (
            f"{self.project.upper()} processed a total of "
            f"{total_throughput:.0f} Gbytes over the last 24 hours of "
            f"measurements."
        )

        kwargs = {
            'title': 'Bandwidth',
            'filename': f'{self.project}_summary_bandwidth.png',
            'text': text,
        }

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)
        ax.set_ylabel('TBytes per Day')

        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the top referers over the last few days.
        """
        df = self.df.copy()
        self.summarize_last_24_hours_transactions(df, html_doc)
        self.summarize_daily_transactions(df, html_doc)

    def summarize_last_24_hours_transactions(self, df, html_doc):
        """
        """
        fig, ax = plt.subplots(figsize=(15, 7))
        
        df = self.df.copy().tail(n=72)
        df = df.set_index('date')
        df = df.resample('T').pad()
        
        # Turn the data from hits/hour to hits/second
        df['mapdraws'] /= 3600
        
        # green
        df['mapdraws'].plot(ax=ax, legend=None, gid='mapdraws', color='#2ca02c')
        
        # Ok, have the mapdraws and the axis in place.  Now add the hits and error
        # information from burst_staging.
        sql = """
              SELECT date,
                     SUM(hits) as hits,
                     SUM(errors) as errors
              FROM burst_staging
              GROUP BY date
              ORDER BY date
              """
        df = pd.read_sql(sql, self.conn)
        df['date'] = pd.to_datetime(df['date'], unit='s')
        
        # This are by the minute, so restrict to last day = 1440 minutes.
        df = df.tail(n=1440 * 3)
        
        df = df.set_index('date')
        
        # Get average rate per second.
        df['hits'] /= 60
        df['errors'] /= 60
        
        # Get the rolling mean of hits and errors.
        dfr = df[['hits', 'errors']].rolling(15).aggregate([np.mean, np.max, np.min])
        max_burst = dfr['hits']['amax'].tail(n=1440).max()
        
        # Line plots for hits and errors.
        dfr['hits']['mean'].plot(ax=ax, gid='hits', color='black')
        # orange
        dfr['errors']['mean'].plot(ax=ax, gid='errors', color='#ff7f03')
        
        # Fill the area between the rolling min and max for hits.  This gives an
        # indication of the short term range.
        time = (dfr.index - pd.datetime(1970,1,1)).total_seconds() / 60
        # facecolor = [0.29803922, 0.44705882, 0.69019608, 1.]
        bounds_artist = ax.fill_between(time, dfr['hits']['amax'], dfr['hits']['amin'],
                                        gid='hits range', zorder=1, edgecolor=None,
                                        facecolor='#1f77b4')

        xlim = ax.get_xlim()

        # get the geoevent information
        sql = """
              SELECT MAX(date) AS date FROM user_agent_logs
              """
        df = pd.read_sql(sql, self.conn)
        max_date = df.loc[0]['date']
        starting_date = max_date - 86400 * 3
        
        sql = """
              SELECT a.date, SUM(a.hits) as hits
              FROM user_agent_logs a
              INNER JOIN known_user_agents b
              ON a.id = b.id
              WHERE b.name LIKE 'GeoEvent%'
              GROUP BY a.date
              ORDER BY a.date
              """
        # df = pd.read_sql(sql, self.conn, params=(max_date,))
        df = pd.read_sql(sql, self.conn)
        df['date'] = pd.to_datetime(df['date'], unit='s')
        df = df.set_index('date')

        # resample to minute
        df = df.resample('T').pad()
        df /= 3600
        # red
        df.plot(ax=ax, label='GeoEvent', gid='GeoEvent', color='#d62728')

        # purple and brown, #9467bd, #8c564b

        ax.set_xlim(xlim)
        
        handles = [
            bounds_artist,
            ax.lines[1],
            ax.lines[0],
            ax.lines[2],
            ax.lines[3],
        ]
        labels = [
            'hits variation',
            'hits mean',
            ax.lines[0].get_gid(),
            ax.lines[2].get_gid(),
            ax.lines[3].get_gid(),
        ]
        ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))

        ax.set_ylabel('Per Second')

        text = (
            "This shows the rolling mean (15 minutes) for the hits and "
            "errors over the last 3 days for which Akamai logs exist.  "
            "The variability surrounding the mean corresponds to the "
            "minimum and maximum hit rates during the 15 minute window.  "
            "The maximum 1-minute burst over the last 24 hours was "
            f"{max_burst:.0f} hits/sec."
        )
        kwargs = {
            'title': 'Throughput Last 72 Hours',
            'filename': f'{self.project}_transactions_last_24hrs.png',
            'restrict_handles': False,
            'text': text,
        }

        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_daily_transactions(self, df, html_doc):
        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df['hits'] = df['hits']
        df = df[['date', 'hits', 'errors', 'mapdraws']]
        df = df.set_index('date')

        # Turn the data from hits/hour to hits/second
        df['hits'] /= 3600
        df['errors'] /= 3600
        df['mapdraws'] /= 3600

        text = (
            "Here are hourly averages of the hits, errors, and mapdraws "
            "over longer timescales."
        )

        kwargs = {
            'title': 'Throughput',
            'filename': f'{self.project}_transactions.png',
            'text': text,
        }

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        ax.set_ylabel('Per Second')

        self.write_html_and_image_output(df, html_doc, **kwargs)
