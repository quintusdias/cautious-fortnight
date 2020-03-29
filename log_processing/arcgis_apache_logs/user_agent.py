# Standard library imports
import datetime as dt
import importlib.resources as ir

# 3rd party library imports
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns

# Local imports
from .common import CommonProcessor
from . import sql

sns.set()


class UserAgentProcessor(CommonProcessor):
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
        """
        super().__init__(**kwargs)

        self.data_retention_days = 7

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the user_agents.
        """
        pass

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.logger.info(f"user agents:  processing {len(df)} raw records")

        columns = ['date', 'user_agent', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Aggregate by the set frequency and user_agent, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'user_agent']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Have to have the same column names as the database.
        df = self.replace_user_agents_with_ids(df)

        df = self.merge_with_database(df, 'user_agent_logs')

        self.to_table(df, 'user_agent_logs')

        # Reset for the next round of records.
        self.records = []

        self.logger.info("user agents:  done processing raw records")

    def replace_user_agents_with_ids(self, df):
        """
        Record any new user agents and get IDs for them.
        """
        self.logger.info('about to update the user agent LUT...')

        # Try upserting all the current referers.  If a user agent is already
        # known, then do nothing.
        sql = f"""
        insert into user_agent_lut (name) values %s
        on conflict on constraint user_agent_exists do nothing
        """
        args = ((x,) for x in df.user_agent.unique())
        psycopg2.extras.execute_values(self.cursor, sql, args, page_size=1000)

        # Get the all the IDs associated with the referers.  Fold then back
        # into our data frame, then drop the referers because we don't need
        # them anymore.
        sql = f"""
               SELECT id, name from user_agent_lut
               """
        known_referers = pd.read_sql(sql, self.conn)

        df = pd.merge(df, known_referers,
                      how='left', left_on='user_agent', right_on='name')

        df = df.drop(['user_agent', 'name'], axis='columns')
        self.logger.info('finished updating the user agent LUT...')
        return df

    def preprocess_database(self):
        """
        Clean out the user agent tables on mondays.
        """
        self.logger.info('preprocessing user agents ...')

        if dt.date.today().weekday() != 0:
            return

        query = ir.read_text(sql, 'prune_user_agents.sql')
        self.logger.info(query)
        self.cursor.execute(query)

        self.logger.info(f'deleted {self.cursor.rowcount} user agents ...')

        self.conn.commit()

    def process_graphics(self, html_doc):
        self.logger.info(f'User agents:  starting graphics...')
        top_user_agents = self.get_top_user_agents()
        self.summarize_user_agents(html_doc)
        self.summarize_transactions(top_user_agents, html_doc)
        self.summarize_bandwidth(top_user_agents, html_doc)
        self.logger.info(f'User agents:  done with graphics...')

    def get_top_user_agents(self):
        # who are the top user_agents for today?
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        sql = f"""
            SELECT SUM(hits) as hits, id
            FROM user_agent_logs
            where date::date = '{yesterday}'
            GROUP BY id
            order by hits desc
            limit 7
            """
        df = pd.read_sql(sql, self.conn, index_col='id')

        return df.index

    def summarize_bandwidth(self, top_user_agents, html_doc):
        """
        Create a PNG showing the top user_agents (bytes) over the last few
        days.
        """
        txt = """
            SELECT
                logs.date,
                SUM(logs.nbytes) / 1024 ^ 3 as nbytes,
                lut.name as user_agent
            FROM user_agent_logs logs
                INNER JOIN user_agent_lut lut ON logs.id = lut.id
            WHERE logs.id in ({top_uas})
            GROUP BY logs.date, lut.name
            ORDER BY logs.date
            """
        sql = txt.format(top_uas=', '.join(str(id) for id in top_user_agents))
        df = pd.read_sql(sql, self.conn)

        df = df.pivot(index='date', columns='user_agent', values='nbytes')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': 'GBytes per Hour',
            'filename': f'{self.project}_user_agents_bytes.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_transactions(self, top_user_agents, html_doc):
        """
        Create a PNG showing the top user_agents over the last few days.
        """

        txt = """
            SELECT
                logs.date,
                -- scale to hits per second
                SUM(logs.hits) / 3600 as hits,
                lut.name as user_agent
            FROM user_agent_logs logs
                INNER JOIN user_agent_lut lut ON logs.id = lut.id
            WHERE logs.id in ({top_uas})
            GROUP BY logs.date, lut.name
            ORDER BY logs.date
            """
        sql = txt.format(top_uas=', '.join(str(id) for id in top_user_agents))
        df = pd.read_sql(sql, self.conn)

        df = df.pivot(index='date', columns='user_agent', values='hits')

        # Order them by max value.
        s = df.max().sort_values(ascending=False)
        df = df[s.index]

        fig, ax = plt.subplots(figsize=(15, 7))
        df.plot(ax=ax)

        kwargs = {
            'title': (
                'Hits per Second (averaged per hour, not including errors)'
            ),
            'filename': f'{self.project}_user_agents_hits.png',
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_user_agents(self, html_doc):
        """
        Calculate

              I) percentage of hits for each user_agent
             II) percentage of hits for each user_agent that are 403s
            III) percentage of total 403s for each user_agent

        Just for the latest day, though.
        """
        sql = r"""
            SELECT
                SUM(logs.hits) as hits,
                SUM(logs.errors) as errors,
                SUM(logs.nbytes) as nbytes,
                lut.name as user_agent
            FROM user_agent_logs logs INNER JOIN user_agent_lut lut using(id)
            where logs.date = '{yesterday}'
            GROUP BY user_agent
            order by hits desc
            """
        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        sql = sql.format(yesterday=yesterday)
        df = pd.read_sql(sql, self.conn, index_col='user_agent')

        total_hits = df['hits'].sum()
        total_bytes = df['nbytes'].sum()
        total_errors = df['errors'].sum()

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
        df = df.head(15)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'user_agents',
            'atext': 'Top UserAgents',
            'h1text': f'Top UserAgents by Hits: {yesterday}'
        }
        self.create_html_table(df, html_doc, **kwargs)
