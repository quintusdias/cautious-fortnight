# Standard libraary imports
import os
import pathlib
import shutil
import tempfile
import unittest

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import RefererProcessor

class TestSuite(unittest.TestCase):

    def setUp(self):
        """
        Create a temporary directory in which to create artifacts (often the
        current directory).
        """
        self.starting_dir = pathlib.Path.cwd()
        tempdir = tempfile.TemporaryDirectory()
        self.tempdir = pathlib.Path(tempdir.name)
        os.chdir(self.tempdir)

        self.dbdir = self.tempdir / 'Documents' / 'arcgis_apache_logs_database'

    def tearDown(self):
        """
        Change back to the starting directory and remove any artifacts created
        during a test.
        """
        os.chdir(self.starting_dir)
        shutil.rmtree(self.tempdir)

    def test_database_not_initialized(self):
        """
        SCENARIO:  The referer database does not exist.

        EXPECTED RESULT:  The referer database is initialized.  There should be
        two tables, "logs" and "known_referers".
        """
        r = RefererProcessor('idpgis', database_dir=self.dbdir)

        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name
              """
        actual = pd.read_sql(sql, r.conn)

        expected = pd.Series(['known_referers', 'logs'], name='name')
        pd.testing.assert_series_equal(actual['name'], expected)
