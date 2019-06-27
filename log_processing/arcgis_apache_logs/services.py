# Standard library imports
import datetime as dt
import re

# 3rd party libraries
from lxml import etree
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor, thousands_fcn, millions_fcn


class ServicesProcessor(CommonProcessor):
    """
    Attributes
    ----------
    regex : object
        Parses arcgis folders, services, types from the request path.
    """
    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(project, **kwargs)

        pattern = r'''
                   /(nowcoast|idpgis).ncep.noaa.gov.akadns.net
                   /arcgis
                   (/rest)?
                   /services
                   /(?P<folder>\w+)
                   /(?P<service>\w+)
                   /.*
                   '''
        self.regex = re.compile(pattern, re.VERBOSE)

        self.records = []

    def process_match(self, apache_match):
        """
        What services were hit?
        """
        timestamp = apache_match.group('timestamp')
        path = apache_match.group('path')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        m = self.regex.match(path)
        if m is None:
            return

        folder = m.group('folder')
        service = m.group('service')

        record = (timestamp, folder, service, 1, error, nbytes)
        self.records.append(record)
        if len(self.records) == self.MAX_RAW_RECORDS:
            self.process_raw_records()

    def flush(self):
        self.process_raw_records()

    def process_raw_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.db_access_count += 1
        msg = f'processing batch of raw records: {self.db_access_count}'
        self.logger.info(msg)

        columns = [
            'date', 'folder', 'service', 'hits', 'errors', 'nbytes'
        ]
        df = pd.DataFrame(self.records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['date'] = pd.to_datetime(df['date'], format=format)

        df['nbytes'] = df['nbytes'].astype(int)

        # Aggregate by the set frequency and service, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'folder', 'service']
        df = df.set_index('date').groupby(groupers).sum()

        df = df.reset_index()

        df['date'] = df['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df = self.replace_folders_and_services_with_ids(df)

        df.to_sql('service_logs', self.conn, if_exists='append', index=False)
        self.conn.commit()

        # Reset
        self.records = []

    def replace_folders_and_services_with_ids(self, df_orig):

        sql = """
              SELECT * from known_services
              """
        known_services = pd.read_sql(sql, self.conn)

        # Get the service IDs
        df = pd.merge(df_orig, known_services,
                      how='left',
                      left_on=['folder', 'service'],
                      right_on=['folder', 'service'])

        # How many services have NaN for IDs?  This must populate the known
        # referers table before going further.
        new = df[['folder', 'service']][df['id'].isnull()]
        new = new.groupby(['folder', 'service']).count().reset_index()

        if len(new) > 0:
            new.to_sql('known_services', self.conn,
                       if_exists='append', index=False)

            sql = """
                  SELECT * from known_services
                  """
            known_services = pd.read_sql(sql, self.conn)
            df = pd.merge(df_orig, known_services,
                          how='left',
                          left_on=['folder', 'service'],
                          right_on=['folder', 'service'])

        df.drop(['folder', 'service'], axis='columns', inplace=True)

        return df

    def process_graphics(self, html_doc):
        self.get_timeseries()
        self.create_services_table(html_doc)
        self.create_services_hits_plot(html_doc)

    def get_timeseries(self):

        sql = """
              SELECT
                  a.date, a.hits, a.errors, a.nbytes, b.folder, b.service
              FROM service_logs a
              INNER JOIN known_services b
              ON a.id = b.id
              """
        df = pd.read_sql(sql, self.conn)

        df = df.groupby(['date', 'folder', 'service']).sum().reset_index()

        # Right now the 'date' column is in timestamp form.  We need that
        # in native datetime.
        df['date'] = pd.to_datetime(df['date'], unit='s')

        self.df = df
        self.df_today = self.df[self.df.date.dt.day == self.df.date.max().day]

    def create_services_hits_plot(self, html_doc):
        """
        Create a PNG showing the services over the last few days.
        """
        folders = self.df_today.folder.unique()

        for folder in folders:

            # Now restrict the hourly data over the last few days to those
            # referers.  Then restrict to valid hits.  And rename valid_hits to
            # hits.
            df = self.df[self.df.folder == folder].copy()

            df['hits'] = df['hits'] - df['errors']
            df = df[['date', 'service', 'hits']]

            df = df.pivot(index='date', columns='service', values='hits')

            # Set the plot order according to the max values of the services.
            df = df[df.max().sort_values().index.values]

            fig, ax = plt.subplots(figsize=(15, 5))

            service_max = df.max()
            folder_max = service_max.max()

            # Drop any services where the total hits are zero.
            df = df.drop(service_max[service_max == 0].index.values, axis=1)

            if folder_max <= 1:
                continue

            df.plot(ax=ax, legend=None)
            # what is the scale of the data?
            if folder_max < 1000:
                # Don't bother setting this.
                pass
            elif folder_max < 1000000:
                formatter = FuncFormatter(thousands_fcn)
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
            ax.legend(handles, labels, loc='center left',
                      bbox_to_anchor=(1, 0.5))

            path = self.root / f'{folder}_hits.png'
            if path.exists():
                path.unlink()

            plt.savefig(path)

            body = html_doc.xpath('body')[0]
            div = etree.SubElement(body, 'div')
            a = etree.SubElement(div, 'a', name=folder)
            h2 = etree.SubElement(div, 'h2')
            h2.text = folder
            etree.SubElement(div, 'img', src=f"{path.stem}{path.suffix}")

            # Link us in to the table of contents.
            ul = body.xpath('.//ul[@id="services"]')[0]
            li = etree.SubElement(ul, 'li')
            a = etree.SubElement(li, 'a', href=f'#{folder}')
            a.text = folder

    def create_services_table(self, html_doc):
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

        df = df.sort_values(by='hits', ascending=False)

        table, table_css = self.extract_html_table_from_dataframe(df)

        # extract the CSS and place into our own document.
        style = html_doc.xpath('head/style')[0]
        style.text = style.text + '\n' + table_css

        body = html_doc.xpath('body')[0]
        etree.SubElement(body, 'hr')
        div = etree.SubElement(body, 'div')
        a = etree.SubElement(div, 'a', name='servicestable')
        h1 = etree.SubElement(div, 'h1')

        # Add to the table of contents.
        toc = html_doc.xpath('body/ul[@class="tableofcontents"]')[0]
        li = etree.SubElement(toc, 'li')
        a = etree.SubElement(li, 'a', href='#servicestable')
        a.text = 'Services Table'
        etree.SubElement(li, 'ul', id='services')

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        h1.text = f'Services by Hits: {yesterday}'
        div.append(table)
