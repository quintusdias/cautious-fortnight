# Standard library imports
import datetime as dt
import pathlib
import sqlite3
import sys

# 3rd party library imports
from lxml import etree
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd
import seaborn as sns


def millions(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.1f}M'

formatter = FuncFormatter(millions)

class ProcessRateLimitingGraphics(object):
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
    def __init__(self, h5file):
        """
        Parameters
        ----------
        path : path or str
            Path to HDF5 file.
        """
        sns.set()
        self.store = pd.HDFStore(h5file)

    def run(self):
        pass

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

    def process_hits(self):
        """
        Calculate

              I) percentage of hits for each referer
             II) percentage of hits for each referer that are 403s
            III) percentage of total 403s for each referer

        Just for the latest day, though.
        """
        df = self.df[self.df.date == self.df.date.max()]

        hits = df['hits'].sum()
        num_403s = df['hits_403s'].sum()
        print('hits', hits)
        print('403s', num_403s)

        df = df[['referer', 'hits', 'hits_403s']].copy()
        df['hits %'] = df['hits'] / hits * 100

        idx = df['hits_403s'].isnull()
        df.loc[idx, ('hits_403s')] = 0
        df['hits_403s'] = df['hits_403s'].astype(np.uint64)

        df['403s: % of all hits'] = df['hits_403s'] / hits * 100
        df['403s: % of all 403s'] = df['hits_403s'] / num_403s * 100

        # Reorder the columns
        reordered_cols = [
            'referer',
            'hits',
            'hits %',
            'hits_403s',
            '403s: % of all hits',
            '403s: % of all 403s'
        ]
        df = df[reordered_cols]

        self.process_hits_output(df)

    def process_hits_output(self, df):

        df.set_index('referer', inplace=True)

        tablestr = (df.sort_values(by='hits', ascending=False).head(n=10)
                      .style
                      .set_table_styles(self.table_styles)
                      .format({
                          'hits': '{:,.0f}',
                          'hits %': '{:.1f}',
                          'hits_403s': '{:,}',
                          '403s: % of all hits': '{:,.1f}',
                          '403s: % of all 403s': '{:,.1f}',
                      })
                      .render())

        table = etree.HTML(tablestr)

        div = etree.SubElement(self.body, 'div')
        hr = etree.SubElement(div, 'hr')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Top 10 Referers by Hits'
        div.append(table)

        div = etree.SubElement(self.body, 'div')
        img = etree.SubElement(div, 'img', src='referers_hits.png')

    def process_bytes_output(self, df):
        """
        Calculate

            I) Bandwidth for each referer as GBs
        """
        df.set_index('referer', inplace=True)

        fncn = lambda d: issubclass(np.dtype(d).type, np.number)

        df['GBytes'] /= (1024 ** 3)

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
        tablestr = (df.sort_values(by='GBytes', ascending=False).head(n=10)
                      .style
                      .set_table_styles(self.table_styles)
                      .format(format)
                      .render())

        table = etree.HTML(tablestr)

        div = etree.SubElement(self.body, 'div')
        hr = etree.SubElement(div, 'hr')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Top 10 Referers by Bytes'
        div.append(table)

    def process_bytes(self):

        df = self.df[self.df.date == self.df.date.max()]

        bytes = df['bytes'].sum()

        df = df[['referer', 'bytes']].copy()
        df['percentage'] = df['bytes'] / bytes * 100

        df.columns = ['referer', 'GBytes', 'percentage']

        self.process_bytes_output(df)

    def create_timeseries(self):

        # Get the top 5 referers by hits for the latest date.  By "top 5
        # referers", we mean no 403s.
        df = self.df[self.df.date == self.df.date.max()]
        df['valid_hits'] = df['hits'] - df['hits_403s']
        df = df.sort_values(by='valid_hits', ascending=False).head(n=7)
        referers = ', '.join([f"\"{x}\"" for x in df['referer'].values])
        sql = f"""
               SELECT referer, hits - hits_403s AS hits, date
               FROM observations
               WHERE referer IN ({referers})
               ORDER by date, referer
               """
        print(sql)

        df = pd.read_sql(sql, self.conn)
        df['date'] = df['date'].apply(lambda x: dt.datetime.fromordinal(x))
        df = df.pivot(index='date', columns='referer', values='hits')

        fig, ax = plt.subplots(figsize=(15,5))
        ax.yaxis.set_major_formatter(formatter)
        df.plot(ax=ax)
        ax.set_title('Non-403 Hits')
        plt.savefig('nowcoast/referers_hits.png')


    def create_pie_chart(self):

        # Get the top 5 referers by hits for the latest date.
        df = self.df[self.df.date == self.df.date.max()]
        df = df.sort_values(by='hits', ascending=False)
        df = df[['referer', 'hits']].set_index('referer')

        n = 5
        hits_others = df.hits.sum() - df.head(n=n).hits.sum()

        df = df[:n].copy()
        df.loc['others'] = hits_others

        plot = df.plot.pie(y='hits', figsize=(5,15), labels=None)
        plt.savefig('nowcoast/pie.png')


    def run(self):
        # self.create_pie_chart()
        self.create_timeseries()
        self.process_hits()
        self.process_bytes()

        path = pathlib.Path('nowcoast/index.html')
        with path.open(mode='wt') as f:
            etree.ElementTree(self.doc).write(str(path))


if __name__ == '__main__':
    h5file = sys.argv[1]
    o = ProcessRateLimitingGraphics(h5file)
    o.run()
