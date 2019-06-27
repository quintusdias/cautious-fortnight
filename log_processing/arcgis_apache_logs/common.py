# Standard library imports
import logging
import pathlib
import sqlite3

# 3rd party library imports
from lxml import etree


def millions_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e6):.2f}M'


def thousands_fcn(x, pos):
    """
    Parameters
    ----------
    x : value
    pos : position
    """
    return f'{(x/1e3):.3f}K'


class CommonProcessor(object):
    """
    Attributes
    ----------
    conn : obj
        database connectivity
    database : path or str
        Path to database
    db_access_count : int
        Number of times the raw data has been collated and written to the
        database.
    frequency : str
        How to resample the dataframe of apache log records.
    project : str
        Either nowcoast or idpgis
    records : list
        Raw records collected, one for each apache log entry.
    """
    def __init__(self, project, logger=None):

        self.project = project

        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)

        self.root = pathlib.Path.home() / 'Documents' / 'arcgis_apache_logs'

        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=True)

        self.database = self.root / f'arcgis_apache.db'
        if not self.database.exists():
            self.conn = self.create_database()
        else:
            self.conn = sqlite3.connect(self.database)

        self.MAX_RAW_RECORDS = 1000000

        self.records = []
        self.db_access_count = 0
        self.frequency = '1H'

        self.table_styles = [
            # This one doesn't look like it's needed.
            dict(selector='table', props=[('border-collapse', 'collapse')]),
            # Each column header has a solid bottom border and some padding.
            dict(selector='th', props=[('border-bottom', '2px solid #069'),
                                       ('padding', '5px 3px')]),
            # Each cell has a less solid bottom border and the same padding.
            dict(selector='td', props=[('text-align', 'right'),
                                       ('border-bottom', '1px solid #069'),
                                       ('padding', '5px 3px')]),
        ]

    def create_database(self):
        """
        Create an SQLITE database for the records.  There will be 6 tables.

        Returns
        -------
        connection object
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        # Create the known referers table.
        sql = """
              CREATE TABLE known_referers (
                  id integer PRIMARY KEY,
                  name text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_referer
              ON known_referers(name)
              """
        cursor.execute(sql)

        # Create the logs table.
        sql = """
              CREATE TABLE referer_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  FOREIGN KEY (id) REFERENCES known_referers(id)
              )
              """
        cursor.execute(sql)

        # Create the known IP addresses table.  The IP addresses must be
        # unique.
        sql = """
              CREATE TABLE known_ip_addresses (
                  id integer PRIMARY KEY,
                  ip_address text,
                  name text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_ip_address
              ON known_ip_addresses(ip_address)
              """
        cursor.execute(sql)

        # Create the IP address logs table.
        sql = """
              CREATE TABLE ip_address_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  FOREIGN KEY (id) REFERENCES known_ip_addresses(id)
              )
              """
        cursor.execute(sql)

        # Create the known services and logs tables.
        sql = """
              CREATE TABLE known_services (
                  id integer PRIMARY KEY,
                  folder text,
                  service text
              )
              """
        cursor.execute(sql)
        sql = """
              CREATE UNIQUE INDEX idx_services
              ON known_services(folder, service)
              """
        cursor.execute(sql)

        sql = """
              CREATE TABLE service_logs (
                  date integer,
                  id integer,
                  hits integer,
                  errors integer,
                  nbytes integer,
                  FOREIGN KEY (id) REFERENCES known_services(id)
              )
              """
        cursor.execute(sql)

        return conn

    def extract_html_table_from_dataframe(self, df):
        """
        Create an HTML <TABLE> from a dataframe.

        Parameters
        ----------
        df : dataframe
            Hits, bytes, and errors for apache logs

        Returns
        -------
            lxml.etree Element describing an HTML <TABLE>
            CSS decribing the table
        """
        format = {
            'hits': '{:,.0f}',
            'hits %': '{:.1f}',
            'GBytes': '{:,.1f}',
            'GBytes %': '{:.1f}',
            'errors': '{:,.0f}',
            'errors: % of all hits': '{:,.1f}',
            'errors: % of all errors': '{:,.1f}',
        }
        tablestr = (df.style
                      .set_table_styles(self.table_styles)
                      .format(format)
                      .render())

        tree_doc = etree.HTML(tablestr)

        table = tree_doc.xpath('body/table')[0]

        table_css = tree_doc.xpath('head/style')[0].text
        return table, table_css
