# Standard libraary imports
import unittest

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import SummaryProcessor
from .test_core import TestCore


@unittest.skip('not yet')
class TestSuite(TestCore):

    def test_summary_database_tables_not_initialized(self):
        """
        SCENARIO:  The database does not exist.

        EXPECTED RESULT:  The database is initialized.  There should be
        three tables.
        """
        r = SummaryProcessor('idpgis')

        sql = """
              SELECT name
              FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name
              """
        actual = pd.read_sql(sql, r.conn)

        table_names = ['burst_staging', 'burst_summary', 'summary']
        expected = pd.Series(table_names, name='name')
        pd.testing.assert_series_equal(actual['name'], expected)
