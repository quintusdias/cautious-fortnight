# Standard libraary imports

# 3rd party library imports
import pandas as pd
import unittest

# Local imports
from arcgis_apache_logs import IPAddressProcessor
from .test_core import TestCore


@unittest.skip('not yet')
class TestSuite(TestCore):

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
