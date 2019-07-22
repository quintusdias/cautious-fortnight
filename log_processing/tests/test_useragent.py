# Standard libraary imports
import datetime as dt
import importlib.resources as ir
import io
import os
import pathlib
import tempfile
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import ApacheLogParser, UserAgentProcessor
from .test_core import TestCore


class TestSuite(TestCore):

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

    def test_user_agent_database_tables_not_initialized(self):
        """
        SCENARIO:  The database does not exist.

        EXPECTED RESULT:  The database is initialized.  There should be
        two tables.
        """
        r = UserAgentProcessor('idpgis')

        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name
              """
        actual = pd.read_sql(sql, r.conn)

        table_names = ['known_user_agents', 'user_agent_logs']
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
        self.initialize_known_services_table(p1.services)
        p1.parse_input()

        df = pd.read_sql('SELECT * FROM user_agent_logs', p1.user_agent.conn)
        num_ua_records = len(df)

        # Update the user agent log records.
        df.loc[0, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.user_agent.data_retention_days)
            - dt.timedelta(hours=1)
        ).timestamp()
        df.loc[1:, 'date'] = (
            dt.datetime.now()
            - dt.timedelta(days=p1.user_agent.data_retention_days)
            + dt.timedelta(hours=1)
        ).timestamp()

        df.to_sql('user_agent_logs', p1.user_agent.conn,
                  if_exists='replace', index=False)
        p1.user_agent.conn.commit()

        # This is the 2nd time around.  The one log record should have been
        # deleted.
        p2 = ApacheLogParser('idpgis')
        p2.user_agent.preprocess_database()

        df = pd.read_sql('SELECT * FROM user_agent_logs', p2.user_agent.conn)
        self.assertEqual(len(df), num_ua_records - 1)
