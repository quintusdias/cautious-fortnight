# Standard libraary imports
import datetime as dt
import importlib.resources as ir
import io
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import ApacheLogParser, RefererProcessor


class TestSuite(unittest.TestCase):

    def setUp(self):
        """
        Create a temporary directory in which to create artifacts (often the
        current directory).
        """

        self.starting_dir = pathlib.Path.cwd()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        os.chdir(self.tempdir.name)

        fake_home_dir = tempfile.TemporaryDirectory()
        self.fake_home_dir = pathlib.Path(fake_home_dir.name)
        self.addCleanup(fake_home_dir.cleanup)

        patchee = 'arcgis_apache_logs.common.pathlib.Path.home'
        self.homedir_patcher = patch(patchee, return_value=self.fake_home_dir)
        self.homedir_patcher.start()

    def tearDown(self):
        """
        Change back to the starting directory and remove any artifacts created
        during a test.
        """
        os.chdir(self.starting_dir)

        self.homedir_patcher.stop()

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
