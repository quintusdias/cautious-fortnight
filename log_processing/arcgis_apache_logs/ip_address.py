# Standard library imports
import datetime as dt
import sqlite3

# 3rd party library imports
from lxml import etree
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor


class IPAddressProcessor(CommonProcessor):

    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(project, **kwargs)

    def process_match(self, apache_match):
        """
        What IP addresses were given?
        """
        timestamp = apache_match.group('timestamp')
        ip_address = apache_match.group('ip_address')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        self.records.append((timestamp, ip_address, 1, error, nbytes))
        if len(self.records) == self.MAX_RAW_RECORDS:
            self.process_raw_records()

    def flush(self):
        """
        If any records are left after going thru each log entry, we need to
        do one more round of raw log processing.
        """
        self.process_raw_records()

    def process_raw_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.db_access_count += 1
        msg = f'processing batch of IP address records: {self.db_access_count}'
        self.logger.info(msg)

        columns = ['date', 'ip_address', 'hits', 'errors', 'nbytes']
        df = pd.DataFrame(self.records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['date'] = pd.to_datetime(df['date'], format=format)
        df['nbytes'] = df['nbytes'].astype(int)

        # Aggregate by the set frequency and referer, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'ip_address']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        df = self.replace_ip_addresses_with_ids(df)

        # Ok, suitable to send to the database now.
        df.to_sql('ip_address_logs', self.conn,
                  if_exists='append', index=False)
        self.conn.commit()

        self.records = []

    def replace_ip_addresses_with_ids(self, df_orig):
        """
        The IP addresses themselves are not to be logged.  Rather, we wish to
        log an ID standing for the IP address.
        """

        sql = """
              SELECT * from known_ip_addresses
              """
        known_ips = pd.read_sql(sql, self.conn)

        # Get the referer IDs
        df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        # How many IP addresses have NaN for IDs?  This must populate the known
        # IP address table before going further.
        unknown_ips = df['ip_address'][df['id'].isnull()].unique()
        if len(unknown_ips) > 0:
            new_ips_df = pd.Series(unknown_ips, name='ip_address').to_frame()
            new_ips_df.to_sql('known_ip_addresses', self.conn,
                              if_exists='append', index=False)

            sql = """
                  SELECT id, ip_address from known_ip_addresses
                  """
            known_ips = pd.read_sql(sql, self.conn)

            df = pd.merge(df_orig, known_ips, how='left', on='ip_address')

        df = df.drop(['ip_address'], axis='columns')
        return df

    def process_graphics(self, html_doc):
        """
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

        self.create_ip_table(top_ips, html_doc)
        self.create_ip_plot_by_hits(top_ips, html_doc)
        self.create_ip_plot_by_nbytes(top_ips, html_doc)

    def create_ip_plot_by_hits(self, top_ips, html_doc):
        df = self.df[self.df['ip_address'].isin(top_ips)]
        df = df.pivot(index='date', columns='ip_address', values='hits')

        # Order the columns by the largest values.
        ordered_cols = df.max().sort_values(ascending=False).index.values
        df = df[ordered_cols]

        fig, ax = plt.subplots(figsize=(15, 5))
        df.plot(ax=ax)

        ax.set_title(f'Top IPs:  Hits per Hour')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        # handles = handles[::-1][:10]
        # labels = labels[::-1][:10]
        ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))

        path = self.root / f'top_ip_hits.png'
        if path.exists():
            path.unlink()

        plt.savefig(path)

        body = html_doc.xpath('body')[0]
        div = etree.SubElement(body, 'div')
        etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

    def create_ip_plot_by_nbytes(self, top_ips, html_doc):
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

        # Order the columns by the largest values.
        ordered_cols = df.max().sort_values(ascending=False).index.values
        df = df[ordered_cols]

        fig, ax = plt.subplots(figsize=(15, 5))
        df.plot(ax=ax)

        ax.set_title(f'Top IPs:  MBytes per Hour')

        # Shrink the axis to put the legend outside.
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.65, box.height])
        handles, labels = ax.get_legend_handles_labels()
        # handles = handles[::-1][:10]
        # labels = labels[::-1][:10]
        ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))

        path = self.root / f'top_ip_nbytes.png'
        if path.exists():
            path.unlink()

        plt.savefig(path)

        body = html_doc.xpath('body')[0]
        div = etree.SubElement(body, 'div')
        etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

    def create_ip_table(self, top_ips, html_doc):
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

        df = df.sort_values(by='hits', ascending=False)

        table, css = self.extract_html_table_from_dataframe(df)

        # Put the CSS into place in our own document.
        style = html_doc.xpath('head/style')[0]
        style.text = style.text + '\n' + css

        body = html_doc.xpath('body')[0]
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div')
        a = etree.SubElement(div, 'a', name='iptable')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        toc = html_doc.xpath('body/ul[@class="tableofcontents"]')[0]
        li = etree.SubElement(toc, 'li')
        a = etree.SubElement(li, 'a', href='#iptable')
        a.text = 'Top IPs Table'

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Top IPs by Hits: {yesterday}'
        div.append(table)

    def get_timeseries(self):

        sql = """
              SELECT a.date, a.hits, a.errors, a.nbytes,
                     b.ip_address
              FROM ip_address_logs a
              INNER JOIN known_ip_addresses b
              ON a.id = b.id
              """
        df = pd.read_sql(sql, self.conn)

        df = df.groupby(['date', 'ip_address']).sum().reset_index()

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day]
