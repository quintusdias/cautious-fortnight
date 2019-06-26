# Standard library imports
import argparse
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

sns.set()

def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'

def thousands_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e3):.3f}K'

class ProcessGraphics(object):
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
    def __init__(self, project):
        """
        Parameters
        ----------
        path : path or str
            Path to CSV file.
        datestr : str
            Eight character string for the date, YYYYMMDD.
        """
        self.project = project

        root = pathlib.Path.home() \
                / 'git'  \
                / 'gis-monitoring'  \
                / 'log_processing'

        self.referer_database_file = root / f'{self.project}_referers.db'
        self.services_database_file = root / f'{self.project}_services.db'
        self.ip_database_file = root / f'{self.project}_ips.db'

        self.doc = etree.Element('html')
        self.head = etree.SubElement(self.doc, 'head')
        self.style = etree.SubElement(self.head, 'style')

        self.body = etree.SubElement(self.doc, 'body')

        self.toc = etree.SubElement(self.body, 'ul')

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

        self.root = pathlib.Path.home() / 'www' / 'analytics' / project
        self.root.mkdir(parents=True, exist_ok=True)

    def create_ip_graphics(self):
        """
        """
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
        
        self.create_ip_table(top_ips)
        self.create_ip_plot_by_hits(top_ips)
        self.create_ip_plot_by_nbytes(top_ips)

    def create_ip_plot_by_hits(self, top_ips):
        df = self.df[self.df['ip_address'].isin(top_ips)]
        df = df.pivot(index='date', columns='ip_address', values='hits')

        # Order the columns by the largest values.
        ordered_cols = df.max().sort_values(ascending=False).index.values
        df = df[ordered_cols]

        fig, ax = plt.subplots(figsize=(15,5))
        df.plot(ax=ax)

        ax.set_title(f'Top IPs:  Hits per Hour')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        # handles = handles[::-1][:10]
        # labels = labels[::-1][:10]
        leg = ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    
        path = self.root / f'top_ip_hits.png'
        if path.exists():
            path.unlink()
    
        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        img = etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

    def create_ip_plot_by_nbytes(self, top_ips):
        df = self.df[self.df['ip_address'].isin(top_ips)]
        df['nbytes'] /= (1024 * 1024)
        df = df.pivot(index='date', columns='ip_address', values='nbytes')

        # Order the columns by the largest values.
        ordered_cols = df.max().sort_values(ascending=False).index.values
        df = df[ordered_cols]

        fig, ax = plt.subplots(figsize=(15,5))
        df.plot(ax=ax)

        ax.set_title(f'Top IPs:  MBytes per Hour')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        # handles = handles[::-1][:10]
        # labels = labels[::-1][:10]
        leg = ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    
        path = self.root / f'top_ip_nbytes.png'
        if path.exists():
            path.unlink()
    
        plt.savefig(path)

        div = etree.SubElement(self.body, 'div')
        img = etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

    def create_ip_table(self, top_ips):
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

        df = df.sort_values(by='hits', ascending=False)
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

        tree_doc = etree.HTML(tablestr)

        table = tree_doc.xpath('body/table')[0]

        # extract the CSS and place into our own document.
        css = tree_doc.xpath('head/style')[0]
        self.style.text = self.style.text + '\n' + css.text

        hr = etree.SubElement(self.body, 'hr')
        div = etree.SubElement(self.body, 'div')
        a = etree.SubElement(div, 'a', name='iptable')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#iptable')
        a.text = 'Top IPs Table'

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Top IPs by Hits: {yesterday}'
        div.append(table)

    def create_services_table(self):
        """
        Calculate

              I) percentage of hits for each service
             II) percentage of errors for each service

        Just for the latest day, though.
        """
        df = self.df_today.copy().groupby('service').sum()

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

        percentage_all_errors = df['errors'].sum() / df['hits'].sum() * 100

        df = df.sort_values(by='hits', ascending=False)
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

        tree_doc = etree.HTML(tablestr)

        table = tree_doc.xpath('body/table')[0]

        # extract the CSS and place into our own document.
        css = tree_doc.xpath('head/style')[0]
        self.style.text = self.style.text + '\n' + css.text

        hr = etree.SubElement(self.body, 'hr')
        div = etree.SubElement(self.body, 'div')
        a = etree.SubElement(div, 'a', name='servicestable')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#servicestable')
        a.text = 'Services Table'
        ul = etree.SubElement(li, 'ul', id='services')

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Services by Hits: {yesterday}'
        div.append(table)

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

        percentage_all_errors = df['errors'].sum() / df['hits'].sum() * 100

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
                      .render())

        table_doc = etree.HTML(tablestr)
        table = table_doc.xpath('body/table')[0]

        # extract the CSS and place into our own document.
        css = table_doc.xpath('head/style')[0]
        self.style.text = css.text

        div = etree.SubElement(self.body, 'div')
        hr = etree.SubElement(div, 'hr')
        a = etree.SubElement(div, 'a', name='referers')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#referers')
        a.text = 'Top Referers'

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Top Referers by Hits: {yesterday}'
        div.append(table)

    def get_ip_address_timeseries(self):

        conn = sqlite3.connect(self.ip_database_file)
        df = pd.read_sql('SELECT * FROM observations', conn)

        df = df.groupby(['date', 'ip_address']).sum().reset_index()

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day] 

    def get_services_timeseries(self):

        conn = sqlite3.connect(self.services_database_file)
        df = pd.read_sql('SELECT * FROM observations', conn)

        df = df.groupby(['date', 'folder', 'service']).sum().reset_index()

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day] 

    def get_referer_timeseries(self):

        conn = sqlite3.connect(self.referer_database_file)
        df = pd.read_sql('SELECT * FROM observations', conn)

        df = df.groupby(['date', 'referer']).sum().reset_index()

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

    def create_services_hits_plot(self):
        """
        Create a PNG showing the services over the last few days.
        """
        folders = self.df_today.folder.unique()

        for folder in folders:

            # Now restrict the hourly data over the last few days to those
            # referers.  Then restrict to valid hits.  And rename valid_hits to
            # hits.
            df = self.df[self.df.folder == folder]

            df['hits'] = df['hits'] - df['errors']
            df = df[['date', 'service', 'hits']]
    
            df = df.pivot(index='date', columns='service', values='hits')

            # Set the plot order according to the max values of the services.
            df = df[df.max().sort_values().index.values]
    
            fig, ax = plt.subplots(figsize=(15,5))
    
            service_max = df.max()
            folder_max = service_max.max()

            # Drop any services where the total hits are zero.
            df = df.drop(service_max[service_max == 0].index.values, axis=1)

            if folder_max <=1:
                continue
    
            df.plot(ax=ax, legend=None)
            # what is the scale of the data?
            if folder_max < 1000:
                # Don't bother setting this.
                pass
            elif folder_max < 1000000:
                formatter = FuncFormatter(thousands_fcn)
                #ax.yaxis.set_major_formatter(formatter)
            else:
                formatter = FuncFormatter(millions_fcn)
                ax.yaxis.set_major_formatter(formatter)
    
            ax.set_title(f'{folder} folder:  Hits per Hour')

            # Shrink the axis to put the legend outside.
            box = ax.get_position()
            ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
            handles, labels = ax.get_legend_handles_labels()
            handles = handles[::-1][:7]
            labels = labels[::-1][:7]
            leg = ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    
            path = self.root / f'{folder}_hits.png'
            if path.exists():
                path.unlink()
    
            plt.savefig(path)
    
            div = etree.SubElement(self.body, 'div')
            a = etree.SubElement(div, 'a', name=folder)
            h2 = etree.SubElement(div, 'h2')
            h2.text = folder
            img = etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

            # Link us in to the table of contents.
            ul = self.body.xpath('.//ul[@id="services"]')[0]
            li = etree.SubElement(ul, 'li')
            a = etree.SubElement(li, 'a', href=f'#{folder}')
            a.text = folder

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

    def summarize(self):
        """
        """
        self.get_referer_timeseries()

        s = self.df_today.copy().sum()

        hit_rate = s['hits'] / 86400
        total_bytes = s['nbytes'] / (1024 ** 3)
        text = (
            f"{self.project.upper()} averaged {hit_rate:.0f} hits/second and "
            f"transferred a total of {total_bytes:.1f} GB/day."
        )
        print(text)

        etree.SubElement(self.body, 'hr')
        div = etree.SubElement(self.body, 'div')
        a = etree.SubElement(div, 'a', name='summary')
        h1 = etree.SubElement(div, 'h1')
        h1.text = 'Summary'

        self.summary = etree.SubElement(div, 'p')
        self.summary.text = text

        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a', href='#summary')
        a.text = 'Summary'

    def run(self):
        self.summarize()

        self.get_referer_timeseries()
        self.create_referer_table()
        self.create_referer_hits_plot()
        self.create_referer_bytes_plot()

        self.get_services_timeseries()
        self.create_services_table()
        self.create_services_hits_plot()

        self.get_ip_address_timeseries()
        self.create_ip_graphics()

        path = self.root / 'index.html'
        with path.open(mode='wt') as f:
            etree.ElementTree(self.doc).write(str(path))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    args = parser.parse_args()

    o = ProcessGraphics(args.project)
    o.run()

