#!/usr/bin/env python

# Standard library imports
import datetime as dt

# 3rd party library imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'


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
    time_series_sql : str
        SQL to collect a coherent timeseries of folder/service information.
    """
    def __init__(self, project, **kwargs):
        """
        Parameters
        ----------
        """
        super().__init__(project, **kwargs)

        self.time_series_sql = """
            SELECT a.date, SUM(a.hits) as hits, SUM(a.errors) as errors,
                   SUM(a.nbytes) as nbytes, b.name as user_agent
            FROM user_agent_logs a
            INNER JOIN known_user_agents b
            ON a.id = b.id
            GROUP BY a.date, user_agent
            ORDER BY a.date
            """

        self.data_retention_days = 7

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the user_agents.
        """

        cursor = self.conn.cursor()

        # Do the user_agent tables exist?
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
              """
        df = pd.read_sql(sql, self.conn)

        if 'known_user_agents' not in df.name.values:

            sql = """
                  CREATE TABLE known_user_agents (
                      id integer PRIMARY KEY,
                      name text
                  )
                  """
            cursor.execute(sql)
            sql = """
                  CREATE UNIQUE INDEX idx_user_agent
                  ON known_user_agents(name)
                  """
            cursor.execute(sql)

        if 'user_agent_logs' not in df.name.values:

            sql = """
                  CREATE TABLE user_agent_logs (
                      date integer,
                      id integer,
                      hits integer,
                      errors integer,
                      nbytes integer,
                      CONSTRAINT fk_user_agents_id
                          FOREIGN KEY (id)
                          REFERENCES known_user_agents(id)
                          ON DELETE CASCADE
                  )
                  """
            cursor.execute(sql)

            sql = """
                  CREATE UNIQUE INDEX idx_user_agent_logs_date
                  ON user_agent_logs(date, id)
                  """
            cursor.execute(sql)

    def process_raw_records(self, df):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        columns = ['date', 'user_agent', 'hits', 'errors', 'nbytes']
        df = df[columns].copy()

        # Aggregate by the set frequency and user_agent, taking sums.
        groupers = [pd.Grouper(freq=self.frequency), 'user_agent']
        df = df.set_index('date').groupby(groupers).sum().reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = df['date'].astype(np.int64) // 1e9

        # Have to have the same column names as the database.
        df = self.replace_user_agents_with_ids(df)

        df = self.merge_with_database(df, 'user_agent_logs')

        df.to_sql('user_agent_logs', self.conn,
                  if_exists='append', index=False)
        self.conn.commit()

        # Reset for the next round of records.
        self.records = []

    def replace_user_agents_with_ids(self, df_orig):
        """
        Don't log the actual user_agent names to the database, log the ID
        instead.
        """

        sql = """
              SELECT * from known_user_agents
              """
        known_user_agents = pd.read_sql(sql, self.conn)

        # Get the user_agent IDs
        df = pd.merge(df_orig, known_user_agents,
                      how='left', left_on='user_agent', right_on='name')

        # How many user_agents have NaN for IDs?  This must populate the known
        # user_agents table before going further.
        unknown_user_agents = df['user_agent'][df['id'].isnull()].unique()
        if len(unknown_user_agents) > 0:
            new_df = pd.Series(unknown_user_agents, name='name').to_frame()

            new_df.to_sql('known_user_agents', self.conn,
                          if_exists='append', index=False)

            sql = """
                  SELECT * from known_user_agents
                  """
            known_user_agents = pd.read_sql(sql, self.conn)
            df = pd.merge(df_orig, known_user_agents,
                          how='left', left_on='user_agent', right_on='name')

        df.drop(['user_agent', 'name'], axis='columns', inplace=True)

        return df

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.

        Delete anything older than 7 days.
        """
        if dt.date.today().weekday() != 0:
            # If it's not Monday, do nothing.
            return

        # Ok, it's Monday, drop the IP address tables, they will be recreated.
        cursor = self.conn.cursor()

        sql = """
              DROP INDEX idx_user_agent_logs_date
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP TABLE user_agent_logs
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP INDEX idx_user_agent
              """
        self.logger.info(sql)
        cursor.execute(sql)

        sql = """
              DROP TABLE known_user_agents
              """
        self.logger.info(sql)
        cursor.execute(sql)

        self.conn.commit()

    def process_graphics(self, html_doc):
        self.get_timeseries()
        self.summarize_user_agents(html_doc)
        self.summarize_transactions(html_doc)
        self.summarize_bandwidth(html_doc)

    def get_top_user_agents(self):
        # who are the top user_agents for today?
        df = self.df_today.copy()

        df['valid_hits'] = df['hits'] - df['errors']
        top_user_agents = (df.groupby('user_agent')
                             .sum()
                             .sort_values(by='valid_hits', ascending=False)
                             .head(n=7)
                             .index)

        return top_user_agents

    def summarize_bandwidth(self, html_doc):
        """
        Create a PNG showing the top user_agents (bytes) over the last few
        days.
        """
        top_user_agents = self.get_top_user_agents()

        # Now restrict the hourly data over the last few days to those
        # user_agents.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        idx = self.df.user_agent.isin(top_user_agents)
        df = self.df[idx].sort_values(by='date')
        df = df[['date', 'user_agent', 'nbytes']]
        df['nbytes'] /= (1024 ** 3)

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

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the top user_agents over the last few days.
        """
        top_user_agents = self.get_top_user_agents()

        df = self.df.copy()

        # Now restrict the hourly data over the last few days to those
        # user_agents.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        idx = df.user_agent.isin(top_user_agents)
        df = df[idx].sort_values(by='date').copy()

        df['hits'] = df['hits'] - df['errors']
        df = df[['date', 'user_agent', 'hits']]

        # Rescale them from hits/hour to hits/second
        df['hits'] /= 3600

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
        df = self.df_today.copy().groupby('user_agent').sum()

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
            'hits',
            'hits %',
            'GBytes',
            'GBytes %',
            'errors',
            'errors: % of all hits',
            'errors: % of all errors'
        ]
        df = df[reordered_cols]
        df = df.sort_values(by='hits', ascending=False).head(15)

        yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
        kwargs = {
            'aname': 'user_agents',
            'atext': 'Top UserAgents',
            'h1text': f'Top UserAgents by Hits: {yesterday}'
        }
        self.create_html_table(df, html_doc, **kwargs)
