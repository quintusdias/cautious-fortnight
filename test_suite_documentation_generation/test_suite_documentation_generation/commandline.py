# Standard library imports ...
import argparse
import importlib
import inspect
import pathlib
import re
import sys

# Third party imports.
from lxml import etree as ET

# Local imports
from .testsuite2html import GenerateHtmlFromTestSuite


def ts2html():
    """
    Entry point for ts2html command line tool.
    """

    description = "Generate HTML from test suite module."
    parser = argparse.ArgumentParser(description=description)

    kwargs = {
        'type': str,
        'help': "Path of test module.",
    }
    parser.add_argument('package', **kwargs)

    kwargs = {
        'type': str,
        'help': "Output HTML document.",
    }
    parser.add_argument('output', **kwargs)

    args = parser.parse_args()

    obj = GenerateHtmlFromTestSuite(args.package, args.output)
    obj.run()
