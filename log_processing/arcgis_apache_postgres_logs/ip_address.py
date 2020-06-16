# Standard library imports
import datetime as dt

# 3rd party library imports
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2.extras

# Local imports
from .common import CommonProcessor


class IPAddressProcessor(CommonProcessor):
    """
    Attributes
    ----------
    """

    def __init__(self, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(**kwargs)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.logger.info(f'IP addresses:  processing {len(df)} records...')
        columns = ['date', 'ip_address', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Aggregate by the set frequency and referer, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'ip_address']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        df = self.replace_ip_addresses_with_ids(df)

        df = self.merge_with_database(df, 'ip_address_logs')

        self.logger.info(f'Writing {len(df)} records to ip_address_logs...')
        self.to_table(df, 'ip_address_logs')
        self.logger.info('Done writing to ip_address_logs...')

        self.records = []
        self.logger.info('IP addresses:  done processing records...')

    def replace_ip_addresses_with_ids(self, df):
        """
        The IP addresses themselves are not to be logged.  Rather, we wish to
        log the IDs standing in for the IP address.
        """
        self.logger.info('about to update the IP address LUT...')

        # Try upserting all the current IP addresses.  If an IP address is
        # already known, then do nothing.
        sql = f"""
        insert into ip_address_lut (ip_address) values %s
        on conflict on constraint ip_address_exists do nothing
        """
        args = ((x,) for x in df.ip_address.unique())
        psycopg2.extras.execute_values(self.cursor, sql, args, page_size=1000)

        # Get the all the IDs associated with the IPs.  Fold then back into
        # our data frame, then drop the IPs because we don't need them anymore.
        sql = f"""
               SELECT id, ip_address from ip_address_lut
               """
        known_ips = pd.read_sql(sql, self.conn)

        df = pd.merge(df, known_ips, how='left', on='ip_address')

        df = df.drop(['ip_address'], axis='columns')
        self.logger.info('finished updating the IP address LUT...')
        return df

    def get_top_ip_addresses(self):

        # Get all the top IP addresses as of yesterday.
        sql = """
            -- Summarize the total hits and bytes by IP address for yesterday.
            with today_cte as (
                SELECT
                    SUM(logs.hits) as hits,
                    SUM(logs.nbytes) as nbytes,
                    lut.ip_address
                FROM ip_address_logs logs
                    INNER JOIN ip_address_lut lut using(id)
                where logs.date::date = '{yesterday}'
                GROUP BY lut.ip_address
            )
            -- Get the top 5 IPs by bytes and hits
            (
                select ip_address from today_cte
                order by hits desc
                limit 5
            )
            UNION
            (
                select ip_address from today_cte
                order by nbytes desc
                limit 5
            )
        """
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        sql = sql.format(yesterday=yesterday)
        df = pd.read_sql(sql, self.conn)
        top_ips = df.ip_address.values

        return top_ips

    def process_graphics(self, html_doc):
        """Create the HTML and graphs for the IP addresses.

        Parameters
        ----------
        html_doc : lxml.etree.ElementTree
            HTML document for the logs.
        """
        top_ips = self.get_top_ip_addresses()
        self.summarize_ip_addresses(top_ips, html_doc)
        self.summarize_transactions(top_ips, html_doc)
        self.summarize_bandwidth(top_ips, html_doc)

    def summarize_transactions(self, top_ips, html_doc):
        # Get the hits of the top IP addresses over the last several days.
        # Scale the hits from total per hour to hits/sec.
        query = """
        SELECT
            logs.date,
            logs.hits::real / 3600 as hits,
            lut.ip_address
        FROM ip_address_logs logs INNER JOIN ip_address_lut lut using(id)
        where
            date > '{start_time}'
            and ip_address in {top_ips}
        """
        query = query.format(top_ips=tuple(top_ips),
                             start_time=dt.date.today()-dt.timedelta(days=14))
        df = pd.read_sql(query, self.conn)

        df = df.pivot(index='date', columns='ip_address', values='hits')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'Top IPs:  Hits per Second',
            'filename': f'{self.project}_top_ip_hits.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_bandwidth(self, top_ips, html_doc):
        """
        Create plot of bandwidth usage of top IP addresses.  Scale the
        bandwidth to GBytes.

        Parameters
        ----------
        top_ips : list
            IP addresses with the highest bandwidth.
        html_doc : etree Element
            The plot image is to be inserted into this document.
        """
        query = """
        SELECT
            logs.date,
            logs.nbytes / 1024 ^ 2 as nbytes,
            lut.ip_address
        FROM ip_address_logs logs INNER JOIN ip_address_lut lut using(id)
        where
            date > '{start_time}'
            and ip_address in {top_ips}
        """
        query = query.format(top_ips=tuple(top_ips),
                             start_time=dt.date.today()-dt.timedelta(days=14))
        df = pd.read_sql(query, self.conn)

        df = df.pivot(index='date', columns='ip_address', values='nbytes')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'Top IPs:  MBytes per Hour',
            'filename': f'{self.project}_top_ip_nbytes.png'
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_ip_addresses(self, top_ips, html_doc):

        query = """
            SELECT
                SUM(logs.hits) as hits,
                SUM(logs.nbytes) / 1024 ^ 3 as GBytes,
                SUM(logs.errors) as errors,
                lut.ip_address
            FROM ip_address_logs logs
                INNER JOIN ip_address_lut lut using(id)
            where
                logs.date::date = '{yesterday}'
                and ip_address in {top_ips}
            GROUP BY lut.ip_address
            order by hits desc
        """
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        query = query.format(yesterday=yesterday, top_ips=tuple(top_ips))
        df = pd.read_sql(query, self.conn, index_col='ip_address')

        total_hits = df['hits'].sum()
        total_gbytes = df['gbytes'].sum()
        total_errors = df['errors'].sum()

        df['hits %'] = df['hits'] / total_hits * 100
        df['GBytes %'] = df['gbytes'] / total_gbytes * 100

        df['errors: % of all hits'] = df['errors'] / total_hits * 100
        df['errors: % of all errors'] = df['errors'] / total_errors * 100

        # Reorder the columns
        reordered_cols = [
            'hits',
            'hits %',
            'gbytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'iptable',
            'atext': 'Top IPs Table',
            'h1text': f'Top IP Addresses by Hits: {yesterday}',
        }
        self.create_html_table(df, html_doc, **kwargs)

    def preprocess_database(self, num_days=14):
        """
        Delete any IP addresses with no recent activity.

        Parameters
        ----------
        num_days : int, optional
            Delete user agent entries older than this many days.
        """
        self.logger.info('preprocessing IP addresses ...')

        sql = f"""
            delete from ip_address_logs
            where date < current_date - interval '{num_days} days'
        """
        self.logger.info(sql)
        self.cursor.execute(sql)
        self.logger.info(f'deleted {self.cursor.rowcount} records ...')

        sql = """
            delete from ip_address_lut
            where id not in (
                select distinct id from ip_address_logs
            )
            """
        self.logger.info(sql)
        self.cursor.execute(sql)

        self.logger.info(f'deleted {self.cursor.rowcount} IP addresses ...')

        self.conn.commit()
