#!/usr/bin/env python

# Standard library imports

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
    """
    def __init__(self, **kwargs):
        """
        """
        super().__init__(**kwargs)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.
        """
        # Drop any records from the burst staging table that are older than
        # a certain number of days.
        sql = """
              DELETE FROM burst
              WHERE date < current_date - interval '14 days'
              """
        self.cursor.execute(sql)

        self.conn.commit()

    def process_raw_records(self, raw_df):

        self.logger.info(f'Summary:  processing {len(raw_df)} records...')
        columns = ['date', 'hits', 'errors', 'nbytes']
        df = raw_df[columns].copy()

        # Do the burst summary (1 minute)
        df = (df.set_index('date')
                .resample('T')
                .sum()
                .reset_index())
        self.to_table(df, 'burst')

        # Do the hourly summary
        df = raw_df[columns].copy()

        # As a last step, aggregate the data without regard to the referer.
        df = (
            df.set_index('date')
              .resample(self.frequency)
              .sum()
              .reset_index()
        )

        df = self.merge_with_database(df, 'summary')

        # Now merge with the map draw information from the services table.
        starting_date = pd.Timestamp(df.loc[0]['date'])
        sql = f"""
              SELECT date,
                     SUM(export_mapdraws) as export_mapdraws,
                     SUM(wms_mapdraws) as wms_mapdraws
              FROM service_logs
              WHERE date >= %(date)s
              GROUP BY date
              """
        df_svc = pd.read_sql(sql, self.conn, params={'date': starting_date})
        df = pd.merge(df, df_svc, on='date', how='left')
        df = df.fillna(value=0)

        df['mapdraws'] = df['export_mapdraws'] + df['wms_mapdraws']
        df = df.drop(['export_mapdraws', 'wms_mapdraws'], axis='columns')

        self.to_table(df, 'summary')

        self.logger.info('Summary:  done processing records...')

    def process_graphics(self, html_doc):

        self.logger.info(f'Summary:  starting graphics...')

        body = html_doc.xpath('body')[0]
        div = lxml.etree.SubElement(body, 'div')
        lxml.etree.SubElement(div, 'hr')
        h1 = lxml.etree.SubElement(div, 'h1')
        h1.text = f"{self.project.upper()} Summary"

        self.summarize_last_24_hours_transactions(html_doc)
        self.summarize_daily_transactions_longer_span(html_doc)

        self.summarize_bandwidth(html_doc)

        self.logger.info(f'Summary:  done with graphics...')

    def summarize_bandwidth(self, html_doc):
        """
        Create an image showing the bandwidth over the last few days.
        """
        sql = """
            SELECT
                -- resample to dates
                date::date,
                -- scale to TBytes
                SUM(nbytes) / 1024 ^ 4 as bandwidth
            FROM summary
            GROUP BY date::date
            ORDER BY date::date
            """
        df = pd.read_sql(sql, self.conn, index_col='date')

        throughput_last24h = df.iloc[-1]['bandwidth'] * 1000

        text = (
            f"{self.project.upper()} processed a total of "
            f"{throughput_last24h:.0f} Gbytes over the last 24 hours of "
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

    def get_hourly_mapdraws_over_last_72_hours(self):
        """
        Technically, this is the last 72 hours for which we have data.  If
        there are gaps in the logs, then the data will extend back further.
        """
        sql = """
            SELECT
                date,
                SUM(mapdraws) / 3600 as mapdraws
            FROM summary
            GROUP BY date
            ORDER BY date desc
            -- Last 3 days = 72 hours
            limit 72
            """
        df = pd.read_sql(sql, self.conn, index_col='date')
        df = df.resample('min').pad()
        return df

    def get_by_the_minute_hits_and_errors(self):
        sql = f"""
              SELECT date,
                     -- scale hits and errors to hits / sec
                     SUM(hits) / 60 as hits,
                     SUM(errors) / 60 as errors
              FROM burst
              GROUP BY date
              -- last 3 days
              ORDER BY date desc
              limit 4320
              """
        df = pd.read_sql(sql, self.conn, index_col='date')
        return df

    def get_geoevent_information(self):
        sql = """
              SELECT
                  logs.date,
                  -- scale to hits / sec
                  SUM(logs.hits) / 3600 as hits
              FROM user_agent_logs logs
                  INNER JOIN user_agent_lut lut using(id)
              WHERE lut.name LIKE 'GeoEvent%'
              GROUP BY logs.date
              -- last 3 days
              ORDER BY logs.date desc
              limit 72
              """
        df = pd.read_sql(sql, self.conn, index_col='date')
        return df

    def summarize_last_24_hours_transactions(self, html_doc):
        """
        """
        fig, ax = plt.subplots(figsize=(15, 7))

        # green mapdraws
        df = self.get_hourly_mapdraws_over_last_72_hours()
        df['mapdraws'].plot(ax=ax, legend=None, gid='mapdraws',
                            color='#2ca02c')

        # Ok, have the mapdraws and the axis in place.  Now add the hits and
        # error information from burst table.
        df_min = self.get_by_the_minute_hits_and_errors()

        # Get the rolling mean of hits and errors.
        dfr = df_min.rolling(15).aggregate([np.mean, np.max, np.min])

        # Line plots for hits and errors.
        dfr['hits']['mean'].plot(ax=ax, gid='hits', color='black')
        # orange
        dfr['errors']['mean'].plot(ax=ax, gid='errors', color='#ff7f03')

        # Fill the area between the rolling min and max for hits.  This gives
        # an indication of the short term range.
        time = (dfr.index - pd.Timestamp(0, tz='UTC')).total_seconds() / 60

        # facecolor = [0.29803922, 0.44705882, 0.69019608, 1.]
        bounds_artist = ax.fill_between(time, dfr['hits']['amax'],
                                        dfr['hits']['amin'],
                                        gid='hits range', zorder=1,
                                        edgecolor=None, facecolor='#1f77b4')

        xlim = ax.get_xlim()

        df_geoevent = self.get_geoevent_information()
        df_geoevent = df_geoevent.resample('min').pad()

        # red
        df_geoevent.plot(ax=ax, label='GeoEvent', gid='GeoEvent',
                         color='#d62728')

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

        max_burst_1min = df_min['hits'].head(n=1440).max()
        text = (
            "This shows the rolling mean (15 minutes) for the hits and "
            "errors over the last 3 days for which Akamai logs exist.  "
            "The variability surrounding the mean corresponds to the "
            "minimum and maximum hit rates during the 15 minute window.  "
            "The maximum 1-minute burst over the last 24 hours was "
            f"{max_burst_1min:.0f} hits/sec."
        )
        kwargs = {
            'title': 'Throughput Last 72 Hours',
            'filename': f'{self.project}_transactions_last_24hrs.png',
            'restrict_handles': False,
            'text': text,
        }

        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_daily_transactions_longer_span(self, html_doc):
        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        sql = """
            SELECT
                date::date,
                -- scale to hits / second
                SUM(hits) / 86400 as hits,
                SUM(errors) / 86400 as errors,
                SUM(mapdraws) / 86400 as mapdraws
            FROM summary
            GROUP BY date::date
            ORDER BY date::date desc
            """
        df = pd.read_sql(sql, self.conn, index_col='date')

        text = (
            "Here are daily averages of the hits, errors, and mapdraws "
            "over the full timescale upon which data is available."
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
