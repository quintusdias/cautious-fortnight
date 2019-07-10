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
from arcgis_apache_logs import ApacheLogParser, IPAddressProcessor


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

    def test_database_not_initialized(self):
        """
        SCENARIO:  The referer database does not exist.

        EXPECTED RESULT:  The IP address database is initialized.
        There should be two tables, "logs" and "known_ip_addresses".
        """
        r = IPAddressProcessor('idpgis')

        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name
              """
        actual = pd.read_sql(sql, r.conn)

        table_names = ['ip_address_logs', 'known_ip_addresses']
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

        df = pd.read_sql('SELECT * FROM ip_address_logs', p1.ip_address.conn)
        num_ip_address_records = len(df)

        # Update the ip_address log records.
        df.loc[0, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.ip_address.data_retention_days)
            - dt.timedelta(hours=1)
        ).timestamp()
        df.loc[1:, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.ip_address.data_retention_days)
            + dt.timedelta(hours=1)
        ).timestamp()

        df.to_sql('ip_address_logs', p1.ip_address.conn,
                  if_exists='replace', index=False)
        p1.ip_address.conn.commit()

        # This is the 2nd time around.  The one log record should have been
        # deleted.
        p2 = ApacheLogParser('idpgis')
        p2.ip_address.preprocess_database()

        df = pd.read_sql('SELECT * FROM ip_address_logs', p2.ip_address.conn)
        self.assertEqual(len(df), num_ip_address_records - 1)
