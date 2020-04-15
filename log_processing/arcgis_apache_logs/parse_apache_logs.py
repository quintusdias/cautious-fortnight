# standard library imports
import gzip
import importlib.resources as ir
import logging
import pathlib
import re

# 3rd party library imports
import lxml.etree
import pandas as pd
import psycopg2
import requests

# local imports
from .ip_address import IPAddressProcessor
from .referer import RefererProcessor
from .services import ServicesProcessor
from .summary import SummaryProcessor
from .user_agent import UserAgentProcessor
from . import sql


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
    def __init__(self, project, dbname='arcgis_logs', infile=None):
        """
        Parameters
        ----------
        graphics : bool
            Whether or not to produce any plots or HTML output.
        """
        self.project = project
        self.infile = infile

        self.root = pathlib.Path.home() / 'Documents' / 'arcgis_apache_logs'

        self.schema = project

        if dbname is not None:
            self.conn = psycopg2.connect(dbname=dbname)
            self.cursor = self.conn.cursor()
            self.cursor.execute(f"set search_path to {self.schema}")
        else:
            self.conn, self.cursor = None, None

        self.setup_logger()
        self.setup_regex()

        kwargs = {
            'logger': self.logger,
            'schema': self.schema,
            'conn': self.conn,
            'cursor': self.cursor,
        }
        self.ip_address = IPAddressProcessor(**kwargs)
        self.referer = RefererProcessor(**kwargs)
        self.services = ServicesProcessor(**kwargs)
        self.summarizer = SummaryProcessor(**kwargs)
        self.user_agent = UserAgentProcessor(**kwargs)

        self.graphics_setup()

    def __del__(self):

        # When cleaning up, if we had a connection, commit just to be sure.
        if hasattr(self, 'conn') and self.conn is not None:
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

    def initialize_ag_ap_pg_database(self):
        """
        Examine the project web site and populate the services database with
        existing services.
        """
        commands = ir.read_text(sql, f'init_{self.schema}.sql')
        self.cursor.execute(commands)

        self.cursor.execute(f"set search_path to {self.schema}")

        self.update_ag_ap_pg_services()

        self.conn.commit()

    def upsert_new_folders(self, df):

        folders = df['folder'].unique()

        sql = f"""
        insert into folder_lut (folder)
        values (%(folder)s)
        on conflict on constraint folder_exists do nothing
        """
        for folder in folders:
            self.cursor.execute(sql, {'folder': folder})
            if self.cursor.rowcount == 1:
                self.logger.info(f"Upserted {folder}")

    def retrieve_services_from_database(self):
        """
        What are the existing services that we already have in the database?
        """

        sql = """
        SELECT
            lu_f.folder,
            lu_f.id as folder_id,
            lu_s.service,
            lu_s.id as service_id,
            lu_s.service_type as service_type,
            lu_s.active
        from folder_lut lu_f
            inner join service_lut lu_s on lu_f.id = lu_s.folder_id
        """
        return pd.read_sql(sql, self.conn)

    def check_ag_ap_pg_services(self):
        """
        Check the services lookup table against any new services.
        DO NOT do any UPSERTS.
        """
        self.logger.info("Retrieving services from NCO...")
        nco_df = self.retrieve_services_from_nco()

        self.logger.info("Retrieving services from database...")
        db_df = self.retrieve_services_from_database()

        new_services, retired_services = self.get_changes(nco_df, db_df)

        if len(new_services) > 0:
            print(f'these services are new:  {new_services}')
        else:
            print('there are no new services')

        if len(retired_services) > 0:
            print(f'these services seem to have been retired:')
            for t in retired_services:
                print(f"\t{t[0]}/{t[1]}/{t[2]}")
        else:
            print('no services have been dropped')

    def get_changes(self, nco_df, db_df):
        """
        Determine what's new, what needs to be retired.
        """
        # any new folders?
        nco_services = set((r.folder, r.service, r.service_type)
                           for _, r in nco_df.iterrows())
        db_services = set((r.folder, r.service, r.service_type)
                          for _, r in db_df.iterrows() if r['active'])

        new_diff = nco_services.difference(db_services)
        old_diff = db_services.difference(nco_services)

        return new_diff, old_diff

    def create_any_new_services(self, nco_df):

        self.upsert_new_folders(nco_df)

        # update the dataframe with folder and service type IDs
        df_folders = pd.read_sql("select * from folder_lut", self.conn)
        df = pd.merge(nco_df, df_folders,
                      how='inner', left_on='folder', right_on='folder')

        df = df[['id', 'service', 'service_type']]
        df.columns = ['folder_id', 'service', 'service_type_id']

        # And finally, insert any new services.
        sql = f"""
        insert into service_lut (folder_id, service, service_type)
        values (%(folder_id)s, %(service)s, %(service_type_id)s::svc_type_enum)
        on conflict on constraint service_exists do nothing
        """
        for _, r in df.iterrows():
            self.cursor.execute(sql, r.to_dict())
            if self.cursor.rowcount == 1:
                svc = f"{r['folder_id']}/{r['service']}/{r['service_type_id']}"
                self.logger.info(f"Upserted {svc}")

    def update_ag_ap_pg_services(self):
        """
        Update the services lookup table with any new services.
        Use an UPSERT so that existing rows are left alone.
        """
        self.logger.info("Retrieving services from NCO...")
        nco_df = self.retrieve_services_from_nco()

        self.create_any_new_services(nco_df)

        self.logger.info("Retrieving services from database...")
        db_df = self.retrieve_services_from_database()

        _, retired_services = self.get_changes(nco_df, db_df)

        sql = """
        update service_lut
        set active = false
        where id = %(id)s
        """
        for folder, service, service_type in retired_services:
            query = (
                'folder == @folder '
                'and service == @service '
                'and service_type == @service_type'
            )
            row = db_df.query(query)
            service_id = int(row.iloc[0]['service_id'])

            msg = (
                f"deactivating {row.iloc[0]['folder']}"
                f"/{row.iloc[0]['service']}"
                f"/{row.iloc[0]['service_type']}"
            )
            self.logger.info(msg)
            self.cursor.execute(sql, {'id': service_id})

    def retrieve_services_from_nco(self):
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

        self.logger = logging.getLogger('AGS Apache PG')
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
        self.logger.info('preprocessing the database...')

        # self.summarizer.preprocess_database()
        self.ip_address.preprocess_database()
        self.referer.preprocess_database()
        # self.services.preprocess_database()
        self.user_agent.preprocess_database()

        self.logger.info('done preprocessing the database...')

    def setup_regex(self):
        """
        Create the regular expression for parsing apache logs.
        """
        pattern = r'''
            (?P<ip_address>[a-z\d.:]+)
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
            # match anything but a space
            (?P<path>[^ ]+)
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
            # Match anything but a double quote followed by a space
            "(?P<referer>.*?(?=" ))"
            \s
            # user agent
            # Match anything but a double quote.
            "(?P<user_agent>.*?)"
            \s
            # something else that seems to always be "-"
            "-"
            '''
        self.regex = re.compile(pattern, re.VERBOSE)

    def parse_input(self):
        """
        Process the entire log file.
        """
        self.logger.info('And so it begins...')
        if self.infile is None:
            return

        records = []

        self.logger.info('parsing the logs...')
        for line in gzip.open(self.infile, mode='rt', errors='replace'):
            m = self.regex.match(line)
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

        self.logger.info('done parsing the logs...')

        columns = [
            'date', 'ip_address', 'path', 'hits', 'status_code', 'nbytes',
            'referer', 'user_agent'
        ]
        df = pd.DataFrame.from_records(records, columns=columns)

        df['date'] = pd.to_datetime(df['date'], format='%d/%b/%Y:%H:%M:%S',
                                    utc=True)

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
