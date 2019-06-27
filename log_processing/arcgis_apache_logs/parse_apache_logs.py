# standard library imports
import logging
import pathlib
import re

# 3rd party library imports
import lxml.etree

# local imports
from .ip_address import IPAddressProcessor
from .referer import RefererProcessor
from .services import ServicesProcessor


class ApacheLogParser(object):
    """
    Attributes
    ----------
    conn, cursor : obj
        database connectivity
    database_file : path or str
        Path to database
    infile : file-like
        The apache log file (can be stdin).
    logger : object
        Log any pertinent events.
    apache_regex : object
        Parses lines from the apache log files.
    project : str
        Either nowcoast or idpgis
    """
    def __init__(self, project, infile):
        """
        Parameters
        ----------
        graphics : bool
            Whether or not to produce any plots or HTML output.
        """
        self.project = project
        self.infile = infile

        self.setup_logger()

        pattern = r'''
            # (?P<ip_address>((\d+.\d+.\d+.\d+)|((\w*?:){6}(\w*?:)?(\w+)?)))
            (?P<ip_address>.*?)
            \s
            # Client identity, always -?
            -
            \s
            # Remote user, always -?
            -
            \s
            # Time of request
            \[(?P<timestamp>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s(\+|-)\d{4})\]
            \s
            # The request
            "(?P<request_op>(GET|HEAD|OPTIONS|POST|PROPFIND))
            \s
            (?P<path>.*?)
            \s
            HTTP\/1.1"
            \s
            # Status code
            (?P<status_code>\d+)
            \s
            # payload size
            (?P<nbytes>\d+)
            \s
            # referer
            "(?P<referer>.*?)"
            \s
            # user agent
            "(?P<user_agent>.*?)"
            \s
            # something else that seems to always be "-"
            "-"
            '''
        self.regex = re.compile(pattern, re.VERBOSE)

        self.ip_address = IPAddressProcessor(self.project, logger=self.logger)
        self.referer = RefererProcessor(self.project, logger=self.logger)
        self.services = ServicesProcessor(self.project, logger=self.logger)

        self.root = pathlib.Path.home() / 'Documents' / 'arcgis_apache_logs'

        # Setup a skeleton output document.
        self.doc = lxml.etree.Element('html')
        head = lxml.etree.SubElement(self.doc, 'head')
        style = lxml.etree.SubElement(head, 'style')
        style.text = ''
        body = lxml.etree.SubElement(self.doc, 'body')
        ul = lxml.etree.SubElement(body, 'ul')
        ul.attrib['class'] = 'tableofcontents'

    def setup_logger(self):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        # Create a formatter
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)

        self.logger.addHandler(ch)

    def run(self):

        for line in self.infile:
            m = self.regex.match(line)
            if m is None:
                self.logger.log(line, logging.WARNING)

            self.ip_address.process_match(m)
            self.referer.process_match(m)
            self.services.process_match(m)

        self.ip_address.flush()
        self.referer.flush()
        self.services.flush()

        self.process_graphics()

    def process_graphics(self):

        self.referer.process_graphics(self.doc)
        self.services.process_graphics(self.doc)
        self.ip_address.process_graphics(self.doc)

        # Write the HTML document.
        path = self.root / f'{self.project}.html'
        lxml.etree.ElementTree(self.doc).write(str(path))
