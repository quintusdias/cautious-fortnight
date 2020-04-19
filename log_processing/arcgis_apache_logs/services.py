# Standard library imports
import datetime as dt
import importlib.resources as ir
import re

# 3rd party libraries
from lxml import etree
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Local imports
from .common import CommonProcessor
from . import sql


class ServicesProcessor(CommonProcessor):
    """
    Attributes
    ----------
    regex : object
        Parses arcgis folders, services, types from the request path.
    """
    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(**kwargs)

        pattern = r'''
                   /(nowcoast|idpgis).ncep.noaa.gov.akadns.net
                   /arcgis
                   (?P<rest>/rest)?
                   /services
                   /(?P<folder>\w+)
                   /(?P<service>\w+)
                   /(?P<service_type>\w+)
                   (
                       /
                       (
                           (
                               (?P<export>(export|exportimage))
                               (?P<export_mapdraws>.*?f=image)?
                           )
                           |
                           (
                               (?P<wms>wmsserver)
                               (?P<wms_mapdraws>.*?request=getmap)?
                           )
                       )
                   )?
                   '''
        self.regex = re.compile(pattern, re.VERBOSE | re.IGNORECASE)

        self.records = []

        self.data_retention_days = 180

    def process_raw_records(self, df):
        """
        Turn what we have into a dataframe and aggregate it to the
        appropriate granularity.
        """
        self.logger.info(f'services:  processing {len(df)} raw records...')
        columns = ['date', 'path', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        self.logger.info('services:  regexing service IDs...')
        df_svc = df['path'].str.extract(self.regex)
        self.logger.info('services:  done regexing...')

        # convert the mapdraw columns to integer
        df_svc['wms_mapdraws'] = (~(df_svc.wms_mapdraws.isnull())).astype(int)
        df_svc['export_mapdraws'] = (~(df_svc.export_mapdraws.isnull())).astype(int)  # noqa : E501

        cols = [
            'folder', 'service', 'service_type', 'export_mapdraws',
            'wms_mapdraws'
        ]
        df = pd.concat((df, df_svc[cols]), axis='columns')
        df = df.drop('path', axis='columns')

        # Aggregate by the set frequency and service, taking sums.
        groupers = [
            pd.Grouper(freq=self.frequency),
            'folder', 'service', 'service_type'
        ]
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        if len(df) == 0:
            # no services, so nothing to do
            return

        # Have to have the same column names as the database.
        df = self.replace_folders_and_services_with_ids(df)
        if len(df) == 0:
            return

        df = self.merge_with_database(df, 'service_logs')

        self.to_table(df, 'service_logs')
        self.conn.commit()

        # Reset
        self.records = []

        self.logger.info('services:  processing raw records...')

    def replace_folders_and_services_with_ids(self, df_orig):

        msg = 'selecting folders, services, service_types...'
        self.logger.info(msg)

        sql = f"""
              select
                  s.id id,
                  s.service service,
                  f.folder,
                  s.service_type
              from service_lut s
                   inner join folder_lut f on s.folder_id = f.id
              """
        known_services = pd.read_sql(sql, self.conn)

        msg = 'done selecting folders, services, service_types...'
        self.logger.info(msg)

        group_cols = ['folder', 'service', 'service_type']

        # Get the service IDs
        df = pd.merge(df_orig, known_services,
                      how='left',
                      left_on=group_cols,
                      right_on=group_cols)

        # How many services have NaN for IDs?  These are requests that do not
        # correspond to a valid service.  Normally you might think that these
        # would be logged as errors, but AGS doesn't always do this.
        # This must be dropped.  Maybe they could be folded into the error
        # count somehow.
        dfnull = df[df.id.isnull()]
        n = len(dfnull)
        msg = f"Dropping {n} unmatched IDs, invalid service name given?"
        self.logger.info(msg)
        df = df.dropna(subset=['id'])

        # We have the service ID, we don't need the folder, service, or
        # service_type columns anymore.
        df = df.drop(group_cols, axis='columns')

        return df

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the services.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        self.logger.info('services:  starting graphics...')

        query = ir.read_text(sql, 'services_summary.sql')
        df = pd.read_sql(query, self.conn, index_col='rank')

        # Ensure that the delta columns have zeros instead of None.
        df.day_pct_delta = df.day_pct_delta.fillna(0).astype(np.float64)
        df.week_pct_delta = df.week_pct_delta.fillna(0).astype(np.float64)
        self.create_services_table(df, html_doc)

        # Link in a folder list.
        toc = html_doc.xpath('body/ul[@class="tableofcontents"]')[0]
        li = etree.SubElement(toc, 'li')
        li.text = 'Folders'

        ul = etree.Element('ul', id='services')
        li[:] = [ul]

        self.summarize_transactions(html_doc)

        self.logger.info('services:  finished with graphics...')

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the services over the last few days.

        hits are true hits, i.e. hits minus errors
        """
        query = """
            SELECT
                logs.date,
                f.folder,
                SUM(logs.hits) - SUM(logs.errors) as hits,
                svc.service,
                svc.service_type
            FROM service_logs logs
                INNER JOIN service_lut svc ON logs.id = svc.id
                INNER JOIN folder_lut f on f.id = svc.folder_id
            where logs.date > current_date - interval '2 weeks'
            GROUP BY logs.date, f.folder, svc.service, svc.service_type
            ORDER BY logs.date
        """
        df = pd.read_sql(query, self.conn)

        for folder, df_grp in df.groupby('folder'):

            # If there are services with overlapping names, e.g. CO_OPS
            # mapserver and featureservers, combine the service and
            # service_type columns.  Otherwise drop the service_type column.
            dfg = df_grp.groupby(['service', 'service_type']).count()
            if not np.all(np.diff(dfg.index.codes[0]) > 0):
                # Overlapping names, so collapse the service and service_type
                # columns.
                df_grp.loc[:, 'service'] = (
                    df_grp['service'] + '/' + df_grp['service_type']
                )
            df_grp = df_grp[['date', 'service', 'hits']]

            df_grp = df_grp.pivot(index='date', columns='service',
                                  values='hits')

            # Order them by max value.
            s = df_grp.max().sort_values(ascending=False)
            df_grp = df_grp[s.index]

            service_max = df_grp.max()

            # Drop any services where the total hits too low.
            df_grp = df_grp.drop(service_max[service_max <= 1].index.values,
                                 axis=1)
            if df_grp.shape[1] == 0:
                continue

            if df_grp.max().max() > 3600:
                # Rescale to from hits/hour to hits/second
                df_grp /= 3600
                title = f'{folder} folder:  Hits per second'
            else:
                title = f'{folder} folder:  Hits per hour'

            fig, ax = plt.subplots(figsize=(15, 7))
            df_grp.plot(ax=ax)

            kwargs = {
                'title': title,
                'filename': f'{folder}_hits.png',
                'folder': folder,
            }
            self.write_html_and_image_output(df_grp, html_doc, **kwargs)

    def create_services_table(self, df, html_doc):
        """
        Massage the dataframe a bit before creating an HTML table.
        """
        # Rename some columns.
        # It's awkward to deal with upper case, mixed case, or spaces in column
        # names in postgresql.
        mapper = {
            'gbytes': 'GBytes',
            'gbytes %': 'GBytes %',
            'day_pct_delta': 'Daily Change %',
            'week_pct_delta': 'Weekly Change %',
        }
        df = df.rename(mapper, axis='columns')

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()

        list_items = [
            (
                "\"hits %\" is the ratio of service hits to the total number "
                "of hits, so this column should add to 100."
            ),
            (
                "\"Daily Change %\" is the percentage change in number of "
                "hits since yesterday."
            ),
            (
                "\"Weekly Change %\" is the percentage change in number of "
                "hits in the last seven days from the previous span of seven "
                "days."
            ),
            (
                "\"mapdraw %\" is the ratio of service mapdraws to the "
                "service hits."
            ),

        ]
        kwargs = {
            'aname': 'servicetable',
            'atext': 'Services Table',
            'h1text': f'Services by Hits: {yesterday}',
            'list_items': list_items,
        }
        self.create_html_table(df, html_doc, **kwargs)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.
        """
        pass
