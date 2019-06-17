# Standard library imports
import datetime as dt
import pathlib
import sqlite3
import sys

# 3rd party library imports
from lxml import etree
import matplotlib as mpl
mpl.use('agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
import seaborn as sns


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'

class ProcessReferer(object):
    """
    Attributes
    ----------
    df : Dataframe
        Dataframe for current day of referers.
    database_file : path
        Path to SQLITE3 database.
    date : datetime.date
        Date for the CSV file observations.
    """
    def __init__(self):
        """
        Parameters
        ----------
        path : path or str
            Path to CSV file.
        datestr : str
            Eight character string for the date, YYYYMMDD.
        """
        sns.set()
        self.database_file = pathlib.Path('referer.db')

        self.conn = sqlite3.connect(self.database_file)
        self.cursor = self.conn.cursor()

        self.doc = etree.Element('html')
        self.body = etree.SubElement(self.doc, 'body')

        self.table_styles = [
            # This one doesn't look like it's needed.
            dict(selector='table', props=[('border-collapse', 'collapse')]),
            # Each column header has a solid bottom border and some padding.
            dict(selector='th', props=[('border-bottom', '2px solid #069'),
                                       ('padding', '5px 3px')]),
            # Each cell has a less solid bottom border and the same padding.
            dict(selector='td', props=[('text-align', 'right'),
                                       ('border-bottom', '1px solid #069'),
                                       ('padding', '5px 3px')]),
        ]

        self.root = pathlib.Path.home() / 'www' / 'analytics' / 'referer' / 'nowcoast'
        self.root.mkdir(parents=True, exist_ok=True)

    def create_table(self):
        """
        Calculate

              I) percentage of hits for each referer
             II) percentage of hits for each referer that are 403s
            III) percentage of total 403s for each referer

        Just for the latest day, though.
        """
        df = self.df_today.copy().groupby('referer').sum()

        total_hits = df['hits'].sum()
        total_bytes = df['bytes'].sum()
        total_errors = df['errors'].sum()

        print('hits', total_hits)
        print('errors', total_errors)

        df = df[['hits', 'bytes', 'errors']].copy()
        df['hits %'] = df['hits'] / total_hits * 100
        df['GBytes'] = df['bytes'] / (1024 ** 3)  # GBytes
        df['GBytes %'] = df['bytes'] / total_bytes * 100

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

        percentage_all_errors = df['errors'].sum() / df['hits'].sum() * 100
        print(f"Percentage of Errors:  {percentage_all_errors:.2f}")

        n = 15
        # take the top 15
        df = df.sort_values(by='hits', ascending=False).head(n=n)
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
                      .render())

        table = etree.HTML(tablestr)

        div = etree.SubElement(self.body, 'div')
        hr = etree.SubElement(div, 'hr')
        h1 = etree.SubElement(div, 'h1')

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Top {n} Referers by Hits: {yesterday}'
        div.append(table)

    def process_bytes(self):
        df = self.df_today.copy().groupby('referer').sum()

        total_bytes = df['bytes'].sum()

        df['percentage'] = df['bytes'] / total_bytes * 100

        df = df.sort_values(by='bytes', ascending=False).head(n=10)

        # Calculate
        #
        #    I) Bandwidth for each referer as GBs
        df['GBytes'] = df['bytes'] / (1024 ** 3)
        df = df[['GBytes', 'percentage']]

        format = {
            'GBytes': '{:,.1f}',
            'percentage': '{:.1f}',

        }
        styles = [
            dict(selector='table', props=[('border-collapse', 'collapse')]),
            dict(selector='th', props=[('border-bottom', '2px solid #069'),
                                       ('padding', '5px 3px')]),
            dict(selector='td', props=[('text-align', 'right'),
                                       ('border-bottom', '1px solid #069'),
                                       ('padding', '5px 3px')]),
        ]
        tablestr = (df.style
                      .set_table_styles(self.table_styles)
                      .format(format)
                      .render())

        table = etree.HTML(tablestr)

        div = etree.SubElement(self.body, 'div')
        hr = etree.SubElement(div, 'hr')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Top 10 Referers by Bytes'
        div.append(table)

    def get_timeseries(self):

        df = pd.read_sql('SELECT * FROM observations', self.conn)

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day] 

    def get_top_referers(self):
        # who are the top referers for today?
        df = self.df_today.copy()

        df['valid_hits'] = df['hits'] - df['errors']
        top_referers = df \
                .groupby('referer') \
                .sum() \
                .sort_values(by='valid_hits', ascending=False) \
                .head(n=7) \
                .index

        return top_referers

    def create_timeseries_hits_plot(self):
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

        fig, ax = plt.subplots(figsize=(15,5))

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
        leg = ax.legend(handles[::-1], labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))

        path = self.root / 'referers_hits.png'

        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        img = etree.SubElement(div, 'img', src='referers_hits.png')

    def create_timeseries_bytes_plot(self):
        """
        Create a PNG showing the top referers (bytes) over the last few days.
        """
        top_referers = self.get_top_referers()

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df = self.df[self.df.referer.isin(top_referers)].sort_values(by='date')
        df = df[['date', 'referer', 'bytes']]
        df['bytes'] /= (1024 ** 3)

        df = df.pivot(index='date', columns='referer', values='bytes')

        fig, ax = plt.subplots(figsize=(15,5))

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
        leg = ax.legend(handles[::-1], labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))

        path = self.root / 'referers_bytes.png'

        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        img = etree.SubElement(div, 'img', src='referers_bytes.png')

    def run(self):
        self.get_timeseries()
        self.create_table()
        self.create_timeseries_hits_plot()
        self.create_timeseries_bytes_plot()

        path = self.root / 'index.html'
        with path.open(mode='wt') as f:
            etree.ElementTree(self.doc).write(str(path))


if __name__ == '__main__':
    o = ProcessReferer()
    o.run()
