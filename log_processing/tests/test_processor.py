# Standard library imports
import importlib.resources as ir
import io
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

from arcgis_apache_logs import ApacheLogParser


@patch('arcgis_apache_logs.common.logging.getLogger')
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

    def test_bad_folder(self, mock_logger):
        """
        SCENARIO:  Seven IDPGIS log records are processed as the database is
        initialized.  One of the seven records does not come from a recognized
        folder.  The other six records all come from individual services.

        EXPECTED RESULT:  The known_services table is populated with only six
        records
        """
        text = ir.read_text('tests.data', 'one_invalid_folder.dat')
        s = io.StringIO(text)

        p = ApacheLogParser('idpgis', s)
        p.run()

        conn = p.referer.conn

        df = pd.read_sql("SELECT * from known_services", conn)
        self.assertEqual(df.shape[0], 6)

    def test_init_ten_records(self, mock_logger):
        """
        SCENARIO:  Ten IDPGIS log records are processed as the database is
        initialized.  The first two records are in the same hour, but the
        remaining eight are spread through the day.

        EXPECTED RESULT:  There is a single record in the known_referers table,
        as IDP-GIS has no referer support.  There are 5 records in the
        referers_logs table, as records 0 and 1, 3 and 4, and 6 through 9
        get binned.

        Only eight of the ten requests are for defined services, and two of
        those eight are for the same service, 'watch_warn_adv', but both are
        in the same hour.  So the known_services table gets populated with
        seven records, and the services_logs table gets seven records.

        Three IP addresses have two hits each, so the known_ip_addresses table
        has seven entries while the ip_logs table gets eight entries (the first
        two log entries are from the same source during the same hour, same
        for the 4th and 5th).

        Since the referers are all "-", that means that the number of records
        in the summary table match that of the referer_logs table.

        The logger should have been called at the INFO level a few times.
        """
        text = ir.read_text('tests.data', 'ten.dat')
        s = io.StringIO(text)

        p = ApacheLogParser('idpgis', s)
        p.run()

        conn = p.referer.conn

        df = pd.read_sql("SELECT * from known_referers", conn)
        self.assertEqual(df.shape[0], 1)

        df = pd.read_sql("SELECT * from referer_logs", conn)
        self.assertEqual(df.shape[0], 5)

        df = pd.read_sql("SELECT * from known_services", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from service_logs", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from known_ip_addresses", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from ip_address_logs", conn)
        self.assertEqual(df.shape[0], 8)

        df = pd.read_sql("SELECT * from known_user_agents", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from user_agent_logs", conn)
        self.assertEqual(df.shape[0], 8)

        # The data is resampled, so 23 records, one for each hour.
        df = pd.read_sql("SELECT * from summary", conn)
        self.assertEqual(df.shape[0], 23)

        self.assertTrue(p.logger.info.call_count > 1)

    def test_deleteme(self, mock_logger):
        """
        SCENARIO:  the request path shows the command was DELETEME, which is
        not a recognized HTTP REST command.

        EXPECTED RESULT:  The error logger is invoked.
        """
        text = ir.read_text('tests.data', 'deleteme.dat')
        s = io.StringIO(text)

        p = ApacheLogParser('idpgis', s)
        p.run()

        self.assertEqual(p.logger.warning.call_count, 1)

    def test_out_of_order(self, mock_logger):
        """
        SCENARIO:  The records are out of order.

        EXPECTED RESULT:  The time series retrieved from the database are in
        order.
        """
        text = ir.read_text('tests.data', 'out_of_order.dat')
        s = io.StringIO(text)

        p = ApacheLogParser('idpgis', s)
        p.run()

        p.referer.get_timeseries()

        self.assertTrue(p.referer.df.date.is_monotonic)

    def test_puts(self, mock_logger):
        """
        SCENARIO:  The requests are all "PUT"s.

        EXPECTED RESULT:  The results are recorded, not dropped.
        """
        text = ir.read_text('tests.data', 'put.dat')
        s = io.StringIO(text)

        p = ApacheLogParser('idpgis', s)
        p.run()

        df = pd.read_sql('select * from referer_logs', p.referer.conn)
        self.assertTrue(len(df) > 0)
