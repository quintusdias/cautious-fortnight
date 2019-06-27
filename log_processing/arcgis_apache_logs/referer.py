#!/usr/bin/env python

import datetime as dt
import pathlib
import sqlite3
import urllib.parse

# 3rd party library imports
from lxml import etree
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'


class RefererProcessor(object):
    """
    Attributes
    ----------
    conn, cursor : obj
        database connectivity
    database : path or str
        Path to database
    project : str
        Either nowcoast or idpgis
    """
    def __init__(self, project, database_dir=None):
        """
        Parameters
        ----------
        database_dir : path or None
            Path to directory that will contain the database.
        """

        self.project = project

        if database_dir is None:
            database_dir = (
                pathlib.Path.home() / 'Documents' / 'arcgis_apache_logs'
            )

        if not database_dir.exists():
            database_dir.mkdir(parents=True, exist_ok=True)

        self.database = database_dir / f'tmp_{project}_referers.db'
        if not self.database.exists():
            self.conn = self.create_database()
        else:
            self.conn = sqlite3.connect(self.database)

        self.MAX_RAW_RECORDS = 1000000
        self._referer_records = []

    def get_referer_timeseries(self):

        conn = sqlite3.connect(self.database)
        df = pd.read_sql('SELECT * FROM observations', conn)

        df = df.groupby(['date', 'referer']).sum().reset_index()

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day]

    def process_graphics(self):
        self.get_referer_timeseries()
        self.create_referer_table()
        self.create_referer_hits_plot()
        self.create_referer_bytes_plot()

    def create_referer_bytes_plot(self):
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

        fig, ax = plt.subplots(figsize=(15, 5))

        df.plot(ax=ax, legend=None)

        # ax.xaxis.set_major_locator(mdates.WeekdayLocator())

        # formatter = mdates.DateFormatter('%b %d')
        # ax.xaxis.set_major_formatter(formatter)
        # plt.setp(ax.xaxis.get_majorticklabels(), rotation=20, ha="right")
        # days_fmt = mdates.DateFormatter('%d\n%b\n%Y')
        # hours = mdates.HourLocator()
        # ax.xaxis.set_minor_locator(hours)

        ax.set_title('GBytes per Hour')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[::-1], labels[::-1], loc='center left',
                  bbox_to_anchor=(1, 0.5))

        path = self.root / 'referers_bytes.png'

        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        etree.SubElement(div, 'img', src='referers_bytes.png')

    def create_referer_hits_plot(self):
        """
        Create a PNG showing the top referers over the last few days.
        """
        top_referers = self.get_top_referers()

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df = self.df[self.df.referer.isin(top_referers)].sort_values(by='date')
        df['hits'] = df['hits'] - df['errors']
        df = df[['date', 'referer', 'hits']]

        df = df.pivot(index='date', columns='referer', values='hits')

        fig, ax = plt.subplots(figsize=(15, 5))

        df.plot(ax=ax, legend=None)

        # ax.xaxis.set_major_locator(mdates.WeekdayLocator())

        # formatter = mdates.DateFormatter('%b %d')
        # ax.xaxis.set_major_formatter(formatter)
        # plt.setp(ax.xaxis.get_majorticklabels(), rotation=20, ha="right")
        # days_fmt = mdates.DateFormatter('%d\n%b\n%Y')
        # hours = mdates.HourLocator()
        # ax.xaxis.set_minor_locator(hours)

        formatter = FuncFormatter(millions_fcn)
        ax.yaxis.set_major_formatter(formatter)

        ax.set_title('Hits per Hour (not including errors)')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles[::-1], labels[::-1], loc='center left',
                  bbox_to_anchor=(1, 0.5))

        path = self.root / 'referers_hits.png'

        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        etree.SubElement(div, 'img', src='referers_hits.png')

    def create_referer_table(self):
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

        print('hits', total_hits)
        print('errors', total_errors)

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

        # Construct the HTML <TABLE>
        tablestr = (df.style
                      .set_table_styles(self.table_styles)
                      .format({
                          'hits': '{:,.0f}',
                          'hits %': '{:.1f}',
                          'GBytes': '{:,.1f}',
                          'GBytes %': '{:.1f}',
                          'errors': '{:,.0f}',
                          'errors: % of all hits': '{:,.1f}',
                          'errors: % of all errors': '{:,.1f}',
                      })
                      .render()
        )

        table_doc = etree.HTML(tablestr)
        table = table_doc.xpath('body/table')[0]

        # extract the CSS and place into our own document.
        css = table_doc.xpath('head/style')[0]
        self.style.text = css.text

        div = etree.SubElement(self.body, 'div')
        etree.SubElement(div, 'hr')
        a = etree.SubElement(div, 'a', name='referers')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#referers')
        a.text = 'Top Referers'

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Top Referers by Hits: {yesterday}'
        div.append(table)

    def create_database(self):
        """
        Create an SQLITE database for the referer records.

        Returns
        -------
        connection object
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        # Create the known referers table.
        sql = """
              CREATE TABLE known_referers (
                  id integer,
                  name text
              )
              """
        cursor.execute(sql)

        # Create the logs table.
        sql = """
              CREATE TABLE logs (
                  date integer,
                  referer_id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  FOREIGN KEY (referer_id) REFERENCES known_referers(id)
              )
              """
        cursor.execute(sql)

        return conn

    def preprocess_database(self):
        """
        We don't want items in the referer database getting too old, it will
        take up too much space.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        # Delete anything too old
        sql = """
              DELETE FROM observations WHERE date < ?
              """
        too_old = dt.datetime.now() - dt.timedelta(days=30)
        cursor.execute(sql, (too_old.timestamp(),))

        conn.commit()

    def process_match(self, apache_match):
        """
        What referers were given?
        """
        timestamp = apache_match.group('timestamp')
        referer = apache_match.group('referer')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        self._referer_records.append((timestamp, referer, 1, error, nbytes))
        if len(self._referer_records) == self.MAX_RAW_RECORDS:
            self.process_raw_referer_records()

    def flush(self):
        self.process_raw_referer_records()

    def process_raw_referer_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.dbaccess_count += 1
        print(f'processing batch of referer records: {self.dbaccess_count}')  # noqa: E501

        columns = ['timestamp', 'referer', 'hits', 'errors', 'nbytes']
        df = pd.DataFrame(self._referer_records, columns=columns)

        # Convert the apache timestamp into a python timestamp.  Make the
        # number of bytes numeric.
        format = '%d/%b/%Y:%H:%M:%S %z'
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=format)
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

        # Aggregate by the hour.
        df = df.groupby([
            df['timestamp'].dt.year,
            df['timestamp'].dt.month,
            df['timestamp'].dt.day,
            df['timestamp'].dt.hour,
            df['referer']
        ]).sum()

        midx_names = ['year', 'month', 'day', 'hour', 'referer']
        df.index = df.index.set_names(midx_names)

        df = df.reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
        df['date'] = df['date'].astype(np.int64) // 1e9
        df = df.drop(['year', 'month', 'day', 'hour'], axis='columns')

        # print(f'Number of hits: {df.hits.sum()}')
        # print(f'Number of errors: {df.errors.sum()}')

        # Ok, suitable to send to the database now.
        conn = sqlite3.connect(self.database)
        df.to_sql('observations', conn, if_exists='append', index=False)
        conn.commit()

        # Reset
        self._referer_records = []
