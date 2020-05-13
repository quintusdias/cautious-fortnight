# standard library imports
import importlib.resources as ir
import unittest

from arcgis_apache_postgres_logs.regexps import (
   apache_common_log_format_regex, path_regex
)


class TestSuiteLogEntry(unittest.TestCase):

    def test_user_agent_with_embedded_double_quotes(self):
        """
        SCENARIO:  the user agent has embedded double quotes

        EXPECTED RESULT:  the regular expression match should not be none
        """

        txt = ir.read_text('tests.data', 'nohrsc_snow_analysis.txt')
        m = apache_common_log_format_regex.match(txt)
        self.assertIsNotNone(m)

    def test_user_agent_with_surrounding_triple_quotes(self):
        """
        SCENARIO:  the user agent is surrounded by triple quotes

        EXPECTED RESULT:  the regular expression match should not be none
        """

        txt = ir.read_text('tests.data', 'triple_quotes.txt')
        m = apache_common_log_format_regex.match(txt)
        self.assertIsNotNone(m)

    def test_user_agent_has_double_quotes(self):
        """
        SCENARIO:  the user agent as embedded double quotes

        EXPECTED RESULT:  the regular expression match should not be none
        """

        txt = ir.read_text('tests.data', 'ua_double_quotes.txt')
        m = apache_common_log_format_regex.match(txt)
        self.assertIsNotNone(m)


class TestSuitePath(unittest.TestCase):

    def test__service__basic(self):
        """
        SCENARIO:  a basic path with an export mapdraw is provided
        provided

        EXPECTED RESULT:  the export group should not be None
        """

        txt = ir.read_text('tests.data', 'nohrsc_export.txt')
        m = path_regex.match(txt)
        self.assertIsNotNone(m.group('export'))
