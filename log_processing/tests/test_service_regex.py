# standard library imports
import importlib.resources as ir
import unittest

from arcgis_apache_logs import ApacheLogParser

class TestSuite(unittest.TestCase):

    def test_user_agent_with_embedded_double_quotes(self):
        """
        SCENARIO:  the user agent has embedded double quotes

        EXPECTED RESULT:  the regular expression match should not be none
        """

        txt = ir.read_text('tests.data', 'nohrsc_snow_analysis.txt')
        o = ApacheLogParser('idpgis', dbname=None)
        m = o.regex.match(txt)
        self.assertIsNotNone(m)

    def test_user_agent_with_surrounding_triple_quotes(self):
        """
        SCENARIO:  the user agent is surrounded by triple quotes

        EXPECTED RESULT:  the regular expression match should not be none
        """

        txt = ir.read_text('tests.data', 'triple_quotes.txt')
        o = ApacheLogParser('idpgis', dbname=None)
        m = o.regex.match(txt)
        self.assertIsNotNone(m)


