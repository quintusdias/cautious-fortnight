# Standard libraary imports

# 3rd party library imports
import pandas as pd

# Local imports
from arcgis_apache_logs import RefererProcessor
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
