#!/usr/bin/env python

# Standard library imports

# 3rd party library imports
import lxml.etree
import pandas as pd
import seaborn as sns

# Local imports
from .common import CommonProcessor

sns.set()


class SummaryProcessor(CommonProcessor):
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
        """
        super().__init__(project, **kwargs)

        self.time_series_sql = """
            SELECT
                date,
                SUM(hits) as hits,
                SUM(errors) as errors,
                SUM(nbytes) as nbytes
            FROM summary
            GROUP BY date
            ORDER BY date
            """

    def verify_database_setup(self):
        """
        Verify that all the database tables are setup properly for managing
        the summary.
        """

        cursor = self.conn.cursor()

        # Do the summary tables exist?
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE
                  type='table'
                  AND name NOT LIKE 'sqlite_%'
              """
        df = pd.read_sql(sql, self.conn)

        if 'summary' not in df.name.values:

            sql = """
                  CREATE TABLE summary (
                      date integer,
                      hits integer,
                      errors integer,
                      nbytes integer
                  )
                  """
            cursor.execute(sql)

            sql = """
                  CREATE UNIQUE INDEX idx_summary_date
                  ON summary(date)
                  """
            cursor.execute(sql)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.
        """
        self.conn.execute('VACUUM')
        self.conn.commit()

    def process_graphics(self, html_doc):

        body = html_doc.xpath('body')[0]
        div = lxml.etree.SubElement(body, 'div')
        lxml.etree.SubElement(div, 'hr')
        h1 = lxml.etree.SubElement(div, 'h1')
        h1.text = f"{self.project.upper()} Summary"

        self.get_timeseries()
        self.summarize_transactions(html_doc)
        self.summarize_bandwidth(html_doc)

    def summarize_bandwidth(self, html_doc):
        """
        Create an image showing the bandwidth over the last few days.
        """
        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df = self.df[['date', 'nbytes']].copy()
        df.loc[:, 'nbytes'] /= (1024 ** 3)
        df = df.set_index('date')

        total_throughput = df.tail(n=24).sum().values[0]
        text = (
            f"{self.project.upper()} processed a total of "
            f"{total_throughput:.0f} Gbytes over the last 24 hours of "
            f"measurements."
        )

        kwargs = {
            'title': 'GBytes per Hour',
            'filename': f'{self.project}_summary_bandwidth.png',
            'text': text,
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)

    def summarize_transactions(self, html_doc):
        """
        Create a PNG showing the top referers over the last few days.
        """
        df = self.df.copy()

        # Now restrict the hourly data over the last few days to those
        # referers.  Then restrict to valid hits.  And rename valid_hits to
        # hits.
        df['hits'] = df['hits'] - df['errors']
        df = df[['date', 'hits']]
        df = df.set_index('date')

        # Turn the data from hits/hour to hits/second
        df['hits'] /= 3600

        # Get the maximum over the last day and overall maximum.
        df_last24 = df.tail(n=24)
        max_last_24hrs = df_last24.max().values[0]
        max_time = df_last24[df_last24.hits == max_last_24hrs].index[0]

        text = (
            f"{self.project.upper()} maxed at {max_last_24hrs:.0f} "
            f"hits/second at {max_time} over the last 24 hours of "
            f"measurements."
        )

        kwargs = {
            'title': (
                'Hits per Second (averaged per hour, not including errors)'
            ),
            'filename': f'{self.project}_transactions.png',
            'text': text,
        }
        self.write_html_and_image_output(df, html_doc, **kwargs)
