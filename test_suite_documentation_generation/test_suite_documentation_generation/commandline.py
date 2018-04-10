# Standard library imports ...
import argparse
import importlib
import inspect
import pathlib
import re
import sys

# Third party imports.
from lxml import etree as ET

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


class GenerateHtmlFromTestSuite(object):
    """
    Attributes
    ----------
    package : str
        Name of test package
    output_file : pathlib.Path
        Name/path of/to output file.
    """
    def __init__(self, path, output_file):

        path = pathlib.Path(path)

        # Assume that the parent must be placed onto the sys.path.  The actual
        # package is the last part of the path.  Right?
        sys.path.insert(0, str(path.parent))
        self.package = path.name

        self.output_file = pathlib.Path(output_file)

    def run(self):
        """
        Generate the HTML describing the tests.
        """
        module = importlib.import_module(self.package)

        doc = ET.Element('html')
        body = ET.SubElement(doc, 'body')
        header = ET.SubElement(body, 'h1')
        header.text = self.package.upper()

        for testname, module in inspect.getmembers(module, predicate=inspect.ismodule):

            print("Module:  {module}".format(module=testname))
            div = self.process_module(testname, module)
            if div is not None:
                body.append(div)

        with self.output_file.open(mode='wt') as f:
            f.write(ET.tostring(doc).decode('utf-8'))

    def process_module(self, name, module):
        """
        Turn a testmodule into HTML documentation.  The module will constitute
        a single <DIV>.

        Parameters
        ----------
        name : str
            Name of test module
        module : module
            The actual test module in question

        Returns
        -------
        lxml.etree.Element
            A <DIV> element containing <TABLE> elements corresponding to each
            testsuite in the module.
        """

        print(name)
        div = ET.Element('div')
        h2 = ET.SubElement(div, 'h2')
        h2.text = name

        for testsuitename, testsuite in inspect.getmembers(module, inspect.isclass):

            if not testsuitename.startswith('Test'):
                continue

            print("    ", testsuitename)

            table = self.process_testsuite(testsuitename, testsuite)
            if table is not None:
                div.append(table)

        br = ET.SubElement(div, 'br')

        # How many TABLES do we have so far?  If at least one, then the DIV
        # is legitimate.
        if div.xpath('count(table)') > 0:
            return div
        else:
            return None

    def process_testsuite(self, testsuitename, testsuite):
        """
        Go through a single testsuite, produce an HTML table.
        """

        table = ET.Element('table')
        table.set('border', '1')
        caption = ET.SubElement(table, 'caption')
        caption.text = inspect.getdoc(testsuite)

        obj = testsuite()

        # Collect the information.
        info = []
        for testname, test in inspect.getmembers(obj, inspect.ismethod):

            testinfo = {}

            if not testname.startswith('test_'):
               continue
            print("        ", testname)
            testinfo['name'] = testname

            test_documentation = inspect.getdoc(test)

            # Working assumption is that there is a leading summary followed
            # by a blank line followed by further documentation that includes
            # a statement of the expected result.
            lst = test_documentation.split('\n\n')
            h1 = lst[0]

            # Remove any leading 'SCENARIO:'
            h1 = re.sub('[A-Z]+:\s+', '', h1)

            testinfo['summary'] = h1

            if len(lst) == 1:
                explanation = ''
            else:
                explanation = '\n\n'.join(lst[1:])

            # Remove any 'EXPECTED RESULT:'
            explanation = re.sub('[A-Z]+\s[A-Z]+:\s+', '', explanation)

            testinfo['explanation'] = explanation
            
            info.append(testinfo)

        # How many rows do we have so far?  If none, then the table is not
        # legit.
        if len(info) == 0:
            return None

        # Add a header line.
        tr = ET.SubElement(table, 'tr')
        th = ET.SubElement(tr, 'th')
        th.text = 'Test Name'
        th = ET.SubElement(tr, 'th')
        th.text = 'Test Scenario'
        th = ET.SubElement(tr, 'th')
        th.text = 'Expected Result'
        
        # Transform the accumulated information into a table tow <TR>
        for item in info:

            tr = ET.SubElement(table, 'tr')
            td = ET.SubElement(tr, 'td')
            td.text = item['name']

            td = ET.SubElement(tr, 'td')
            td.text = item['summary']

            td = ET.SubElement(tr, 'td')
            td.text = item['explanation']

        return table
