# Standard libraary imports
import datetime as dt
import importlib.resources as ir
import io
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import ApacheLogParser, RefererProcessor
from .test_core import TestCore


class TestSuite(TestCore):

    def test_referer_database_tables_not_initialized(self):
        """
        SCENARIO:  The database does not exist.

        EXPECTED RESULT:  The database is initialized.  There should be
        two tables.
        """
        r = RefererProcessor('idpgis')

        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name
              """
        actual = pd.read_sql(sql, r.conn)

        table_names = ['known_referers', 'referer_logs']
        expected = pd.Series(table_names, name='name')
        pd.testing.assert_series_equal(actual['name'], expected)

    @patch('arcgis_apache_logs.common.logging.getLogger')
    def test_data_retention(self, mock_logger):
        """
        SCENARIO:  There are ten records just processed, one that is under
        the data retention threshold, the others are over.

        EXPECTED RESULT:  The older log record should be expunged upon the
        next run.
        """
        # Put ten records into the database.
        text = ir.read_text('tests.data', 'ten.dat')
        s = io.StringIO(text)

        p1 = ApacheLogParser('idpgis', s)
        p1.run()

        df = pd.read_sql('SELECT * FROM referer_logs', p1.referer.conn)
        num_referer_records = len(df)

        # Update the referer log records.
        df.loc[0, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.referer.data_retention_days)
            - dt.timedelta(hours=1)
        ).timestamp()
        df.loc[1:, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.referer.data_retention_days)
            + dt.timedelta(hours=1)
        ).timestamp()

        df.to_sql('referer_logs', p1.referer.conn,
                  if_exists='replace', index=False)
        p1.referer.conn.commit()

        # This is the 2nd time around.  The one log record should have been
        # deleted.
        p2 = ApacheLogParser('idpgis')
        p2.referer.preprocess_database()

        df = pd.read_sql('SELECT * FROM referer_logs', p2.referer.conn)
        self.assertEqual(len(df), num_referer_records - 1)
