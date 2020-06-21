# Standard library imports
import datetime as dt
import urllib.parse

# 3rd party library imports
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


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
    """
    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        known_referers : dataframe
            All referers that have been previously encountered.
        """
        super().__init__(**kwargs)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.logger.info(f'Referers:  processing {len(df)} records...')
        columns = ['date', 'referer', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

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
        df_ref = df.set_index('date').groupby(groupers).sum().reset_index()

        # Have to have the same column names as the database.
        df_ref = self.replace_referers_with_ids(df_ref)

        df_ref = self.merge_with_database(df_ref, 'referer_logs')

        self.logger.info(f'Writing {len(df)} records to referer_logs...')
        self.to_table(df_ref, 'referer_logs')
        self.logger.info('Done writing to referer_logs...')

        # Reset for the next round of records.
        self.records = []

        self.logger.info('Referers:  done processing records...')

    def replace_referers_with_ids(self, df):
        """
        Record any new referers and get IDs for them.
        """
        self.logger.info('about to update the referer LUT...')

        # Try upserting all the current referers.  If a referer is already
        # known, then do nothing.
        sql = f"""
        insert into referer_lut (name) values %s
        on conflict on constraint referer_exists do nothing
        """

        args = ((x,) for x in df.referer.unique())

        psycopg2.extras.execute_values(self.cursor, sql, args, page_size=1000)

        # Get the all the IDs associated with the referers.  Fold then back
        # into our data frame, then drop the referer names because we don't
        # need them anymore.
        sql = f"""
               SELECT id, name from referer_lut
               """
        known_referers = pd.read_sql(sql, self.conn)

        df = pd.merge(df, known_referers,
                      how='left', left_on='referer', right_on='name')

        df = df.drop(['referer', 'name'], axis='columns')
        self.logger.info('finished updating the referer LUT...')
        return df

    def preprocess_database(self, num_days=14):
        """
        Remove any referers without any recent activity.

        Parameters
        ----------
        num_days : int, optional
            Delete user agent entries older than this many days.
        """
        self.logger.info('preprocessing referers ...')

        sql = f"""
            delete from referer_logs
            where date < current_date - interval '{num_days} days';
            """
        self.logger.info(sql)
        self.cursor.execute(sql)
        self.logger.info(f'deleted {self.cursor.rowcount} referers logs...')

        # Now delete the referer LUT items that have no log entries.
        sql = """
            delete from referer_lut
            where id not in (
                select distinct id from referer_logs
            )
            """
        self.logger.info(sql)
        self.cursor.execute(sql)
        msg = f'deleted {self.cursor.rowcount} orphaned referers ...'
        self.logger.info(msg)

        self.conn.commit()

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the referers.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        self.logger.info(f'Referers:  starting graphics...')
        self.logger.info(f'Referers:  summarizing table ...')
        self.summarize_referer_table(html_doc)

        top_referers = self.get_top_referers()
        if len(top_referers) == 0:
            return

        self.logger.info(f'Referers:  summarizing transactions ...')
        self.summarize_transactions(top_referers, html_doc)
        self.logger.info(f'Referers:  summarizing bandwidth ...')
        self.summarize_bandwidth(top_referers, html_doc)

        self.logger.info(f'Referers:  done with graphics...')

    def get_top_referers(self):
        # who are the top referers for yesterday?
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        sql = f"""
            SELECT SUM(hits) as hits, id
            FROM referer_logs logs
            where logs.date::date = '{yesterday}'
            GROUP BY id
            order by hits desc
            limit 7
            """
        df = pd.read_sql(sql, self.conn, index_col='id')

        return df.index

    def summarize_bandwidth(self, top_referers, html_doc):
        """
        Create a PNG showing the top referers (bytes) over the last few days.
        """

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        query = """
            SELECT
                logs.date,
                logs.nbytes::real / 1024 ^ 3 as nbytes,
                lut.name as referer
            FROM referer_logs logs INNER JOIN referer_lut lut using(id)
            where
                date > '{start_time}'
                and lut.id in ({top_referers})
            order by logs.date desc
        """
        top_referers = ', '.join(str(id) for id in top_referers)
        query = query.format(top_referers=top_referers,
                             start_time=dt.date.today()-dt.timedelta(days=14))
        df = pd.read_sql(query, self.conn)

        df = df.pivot(index='date', columns='referer', values='nbytes')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)
        ax.set_xlabel('')

        kwargs = {
            'title': 'GBytes per Hour',
            'filename': f'{self.project}_referers_bytes.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_transactions(self, top_referers, html_doc):
        """
        Create a PNG showing the top referers over the last few days.
        """
        query = """
            SELECT
                logs.date,
                logs.hits::real / 3600 as hits,
                lut.name as referer
            FROM referer_logs logs INNER JOIN referer_lut lut using(id)
            where
                date > '{start_time}'
                and lut.id in ({top_referers})
            order by logs.date desc
        """
        # It's possible to format the referers list wrong if there's only one
        # of them, so take this extra step.
        top_referers = ', '.join(str(id) for id in top_referers)

        query = query.format(top_referers=top_referers,
                             start_time=dt.date.today()-dt.timedelta(days=14))
        df = pd.read_sql(query, self.conn)

        # Pivot the referers into the columns so that we can plot them as
        # time series.
        df = df.pivot(index='date', columns='referer', values='hits')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)
        ax.set_xlabel('')

        kwargs = {
            'title': (
                'Hits per Second (averaged per hour, not including errors)'
            ),
            'filename': f'{self.project}_referers_hits.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_referer_table(self, html_doc):
        """
        Calculate

              I) percentage of hits for each referer
             II) percentage of hits for each referer that are 403s
            III) percentage of total 403s for each referer

        Just for the latest day, though.
        """
        sql = r"""
            SELECT
                SUM(logs.hits) as hits,
                SUM(logs.errors) as errors,
                SUM(logs.nbytes) as nbytes,
                lut.name as referer
            FROM referer_logs logs INNER JOIN referer_lut lut using(id)
            where logs.date = '{yesterday}'
            GROUP BY referer
            order by hits desc
            """
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        sql = sql.format(yesterday=yesterday)
        df = pd.read_sql(sql, self.conn, index_col='referer')

        total_hits = df['hits'].sum()
        total_bytes = df['nbytes'].sum()
        total_errors = df['errors'].sum()

        df = df[['hits', 'nbytes', 'errors']].copy()
        df['hits %'] = df['hits'] / total_hits * 100
        df['GBytes'] = df['nbytes'] / (1024 ** 3)  # GBytes
        df['GBytes %'] = df['nbytes'] / total_bytes * 100

        df['errors: % of all hits'] = df['errors'] / total_hits * 100
        df['errors: % of all errors'] = df['errors'] / total_errors * 100

        # Reorder the columns
        reordered_cols = [
            'hits',
            'hits %',
            'GBytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]

        # Limit our display to the top 15.  Do this here to get all the summary
        # statistics first.
        df = df.head(15)

        kwargs = {
            'aname': 'referers',
            'atext': 'Top Referers',
            'h1text': f'Top Referers by Hits: {yesterday}'
        }
        self.create_html_table(df, html_doc, **kwargs)
