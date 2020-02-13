# standard library imports
import gzip
import logging
import pathlib
import re

# 3rd party library imports
import lxml.etree
import pandas as pd
import psycopg2
import requests
import sqlalchemy

# local imports
from .ip_address import IPAddressProcessor
from .referer import RefererProcessor
from .services import ServicesProcessor
from .summary import SummaryProcessor
from .user_agent import UserAgentProcessor


class ApacheLogParser(object):
    """
    Attributes
    ----------
    database_file : path or str
        Path to database
    infile : file-like
        The apache log file (can be stdin).
    logger : object
        Log any pertinent events.
    project : str
        Either nowcoast or idpgis
    """
    def __init__(self, project, infile=None):
        """
        Parameters
        ----------
        graphics : bool
            Whether or not to produce any plots or HTML output.
        """
        self.project = project
        self.infile = infile

        self.schema = project

        self.conn = psycopg2.connect(dbname='arcgis_logs')
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"set search_path to {self.schema}")

        uri = 'postgres+psycopg2:///arcgis_logs'
        self.engine = sqlalchemy.create_engine(uri)

        self.setup_logger()

        kwargs = {
            'logger': self.logger,
            'schema': self.schema,
            'engine': self.engine,
            'conn': self.conn,
            'cursor': self.cursor,
        }
        self.ip_address = IPAddressProcessor(**kwargs)
        self.referer = RefererProcessor(**kwargs)
        self.services = ServicesProcessor(**kwargs)
        self.summarizer = SummaryProcessor(**kwargs)
        self.user_agent = UserAgentProcessor(**kwargs)

    def __del__(self):
        self.conn.commit()

    def graphics_setup(self):
        # Setup a skeleton output document.
        self.doc = lxml.etree.Element('html')
        head = lxml.etree.SubElement(self.doc, 'head')
        style = lxml.etree.SubElement(head, 'style')
        style.text = ''
        body = lxml.etree.SubElement(self.doc, 'body')
        ul = lxml.etree.SubElement(body, 'ul')
        ul.attrib['class'] = 'tableofcontents'

    def initialize_ag_pg_database(self):
        """
        Examine the project web site and populate the services database with
        existing services.
        """
        self.create_pg_database()

        self.update_ags_services()
        self.conn.commit()

    def create_pg_database(self):
        """
        Create the postgresql database.
        """
        sql = f"""
        DROP SCHEMA {self.schema} CASCADE
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        sql = f"""
        CREATE SCHEMA {self.schema}
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        cmd = f"set search_path to {self.schema}"
        self.logger.info(sql)
        self.cursor.execute(cmd)

        self.create_service_lut()
        self.create_service_logs()

        self.create_ip_address_lut()
        self.create_ip_address_logs()

        self.create_referer_lut()
        self.create_referer_logs()

        self.create_user_agent_lut()
        self.create_user_agent_logs()

        self.create_summary()
        self.create_burst_summary()
        self.create_burst_staging()

        self.conn.commit()

    def create_user_agent_lut(self):

        sql = """
        create table user_agent_lut (
            id     serial primary key,
            name   text
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_referer_lut(self):

        sql = """
        create table referer_lut (
            id     serial primary key,
            name   text
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_ip_address_lut(self):

        sql = """
        create table ip_address_lut (
            id           serial primary key,
            ip_address   text
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_burst_staging(self):

        sql = """
        create table burst_staging (
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_burst_summary(self):

        sql = """
        create table burst_summary (
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_summary(self):

        sql = """
        create table summary (
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint,
            mapdraws         bigint
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

    def create_user_agent_logs(self):

        sql = """
        create table user_agent_logs (
            id               bigint,
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint,
            unique           (id, date),
            foreign key (id) references user_agent_lut (id)
                             on delete cascade
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        comment = (
            "comment on table referer_logs is "
            "'A referer cannot have a summarizing set of statistics at "
            "the same time.'"
        )
        self.cursor.execute(comment)

    def create_referer_logs(self):

        sql = """
        create table referer_logs (
            id               bigint,
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint,
            unique           (id, date),
            foreign key (id) references referer_lut (id)
                             on delete cascade
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        comment = (
            "comment on table referer_logs is "
            "'A referer cannot have a summarizing set of statistics at "
            "the same time.'"
        )
        self.cursor.execute(comment)

    def create_ip_address_logs(self):

        sql = """
        create table ip_address_logs (
            id               bigint,
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint,
            unique           (id, date),
            foreign key (id) references ip_address_lut (id)
                             on delete cascade
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        comment = (
            "comment on table ip_address_logs is "
            "'An IP address cannot have a summarizing set of statistics at "
            "the same time.'"
        )
        self.cursor.execute(comment)

    def create_service_lut(self):

        sql = f"""
        create table service_lut (
            id           serial primary key,
            folder       text,
            service      text,
            service_type text
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        comment = (
            "comment on table service_lut is "
            "'This table should not vary unless there is a new release "
            "at NCEP'"
        )
        self.cursor.execute(comment)

    def create_service_logs(self):

        sql = f"""
        create table service_logs (
            id               bigint,
            date             timestamp,
            hits             bigint,
            errors           bigint,
            nbytes           bigint,
            export_mapdraws  bigint,
            wms_mapdraws     bigint,
            unique           (id, date),
            foreign key (id) references service_lut (id)
                             on delete cascade
        )
        """
        self.logger.info(sql)
        self.cursor.execute(sql)

        comment = (
            "comment on table service_logs is "
            "'Aggregated summary statistics'"
        )
        self.cursor.execute(comment)

        comment = (
            "comment on column service_logs.hits is "
            "'Number of hits aggregated over a set time period (one hour?)'"
        )
        self.cursor.execute(comment)

    def initialize_database(self):
        """
        Examine the project web site and populate the services database with
        existing services.
        """
        df = self.retrieve_services()

        df.to_sql('known_services', self.services.engine, schema=self.schema,
                  index=False, if_exists='append')
        self.services.conn.commit()

    def update_ags_services(self):
        """
        Update the services lookup table with any new services.
        """
        df = self.retrieve_services()
        with self.engine.begin() as conn:
            df.to_sql('service_lut', conn,
                      schema=self.schema, index=False, if_exists='append')

    def retrieve_services(self):
        """
        Examine the project web site and retrieve a list of the services.
        """
        url = f"https://{self.project}.ncep.noaa.gov/arcgis/rest/services"
        params = {'f': 'json'}
        r = requests.get(url, params=params)
        r.raise_for_status()

        j = r.json()
        folders = j['folders']
        records = []
        for folder in folders:

            # Retrieve the JSON metadata for the folder, which will contain
            # the list of all services.
            url = (
                f"https://{self.project}.ncep.noaa.gov"
                f"/arcgis/rest/services/{folder}"
            )
            r = requests.get(url, params=params)
            r.raise_for_status()

            # Save each service.
            j = r.json()
            for item in j['services']:
                folder, service = item['name'].split('/')
                service_type = item['type']
                records.append((folder, service, service_type))

        columns = ['folder', 'service', 'service_type']
        df = pd.DataFrame.from_records(records, columns=columns)
        return df

    def setup_logger(self):

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)

        self.logger.addHandler(ch)

    def preprocess_database(self):
        """
        Do any cleaning necessary before processing any new records.
        """

        self.summarizer.preprocess_database()
        self.ip_address.preprocess_database()
        self.referer.preprocess_database()
        self.services.preprocess_database()
        self.user_agent.preprocess_database()

    def parse_input(self):
        """
        Process the entire log file.
        """
        self.logger.info('And so it begins...')
        if self.infile is None:
            return

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
            # Time of request.  The timezone is always UTC, so don't bother
            # parsing it.
            \[(?P<timestamp>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})\s.....\]
            \s
            # The request
            "(?P<request_op>(GET|DELETE|HEAD|OPTIONS|POST|PROPFIND|PUT))
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
        regex = re.compile(pattern, re.VERBOSE)

        records = []

        for line in gzip.open(self.infile, mode='rt', errors='replace'):
            m = regex.match(line)
            if m is None:
                msg = (
                    f"This line from the apache log files was not matched.\n"
                    f"\n"
                    f"{line}"
                )
                self.logger.warning(msg)
                continue

            # the 4th row is to designate a "hit".
            records.append((
                m.group('timestamp'),
                m.group('ip_address'),
                m.group('path'),
                1,
                int(m.group('status_code')),
                int(m.group('nbytes')),
                m.group('referer'),
                m.group('user_agent')
            ))

        columns = [
            'date', 'ip_address', 'path', 'hits', 'status_code', 'nbytes',
            'referer', 'user_agent'
        ]
        df = pd.DataFrame.from_records(records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S'
        df['date'] = pd.to_datetime(df['date'], format=format)

        df['errors'] = df.eval(
            'status_code < 200 or status_code >= 400'
        ).astype(int)

        self.ip_address.process_raw_records(df)
        self.referer.process_raw_records(df)
        self.services.process_raw_records(df)
        self.user_agent.process_raw_records(df)
        self.summarizer.process_raw_records(df)

        self.logger.info('And so it ends...')

    def process_graphics(self):

        if self.infile is not None:
            # Do not produce graphics when parsing.
            return

        self.summarizer.process_graphics(self.doc)
        self.referer.process_graphics(self.doc)
        self.services.process_graphics(self.doc)
        self.ip_address.process_graphics(self.doc)
        self.user_agent.process_graphics(self.doc)

        # Write the HTML document.
        path = self.root / f'{self.project}.html'
        lxml.etree.ElementTree(self.doc).write(str(path))
