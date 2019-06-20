#!/usr/bin/env python

import argparse
import collections
import operator
import pathlib
import re
import sqlite3
import sys
import urllib.parse

# 3rd party library imports
import pandas as pd

class LogProcessor(object):
    """
    Attributes
    ----------
    conn, cursor : obj
        database connectivity
    database_file : path or str
        Path to database
    infile : file-like
        The apache log file (can be stdin).
    apache_regex : object
        Parses lines from the apache log files.
    path_regex : object
        Parses arcgis folders and services from the request path.
    project : str
        Either nowcoast or idpgis
    """
    def __init__(self, project, infile):
        self.project = project
        self.infile = infile

        self.services_database = pathlib.Path(f'{project}_services.db')
        if not self.services_database.exists():
            self.create_services_database()
        else:
            self.conn = sqlite3.connect(self.services_database)
            self.cursor = self.conn.cursor()

        self.apache_regex = re.compile(r'''
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
            "(?P<request_op>(GET|HEAD|OPTIONS|POST))
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
            ''',
            re.VERBOSE
        )
        self.path_regex = re.compile(r'''
            /(nowcoast|idpgis).ncep.noaa.gov.akadns.net
            /arcgis
            (/rest)?
            /services
            /(?P<folder>\w+)
            /(?P<service>\w+)
            /.*
            ''',
            re.VERBOSE
        )

        self.MAX_RAW_RECORDS = 1000000
        self._service_records = []
        self._intermediate_service_summaries = []

    def create_services_database(self):
        """
        Create an SQLITE database for the service records.
        """
        self.conn = sqlite3.connect(self.services_database)
        self.cursor = self.conn.cursor()

        sql = """
            CREATE TABLE observations (
                date text,
                service text,
                hits integer,
                nbytes integer
            )
            """
        self.cursor.execute(sql)

    def run(self):
        for line in self.infile:
            m = self.apache_regex.match(line)
            if m is None:
                print(line)
            self.process_request_path(m)

        # Any intermediate processing left to do?
        if len(self._service_records) > 0:
            self.process_raw_service_records()

        self.finalize_service_records()
        # self.summarize_services()

    def process_request_path(self, apache_match):
        """
        What services were hit?
        """
        path = apache_match.group('path')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        if status_code < 200 and status_code >= 400:
            return

        m = self.path_regex.match(path)
        if m is None:
            return

        self._service_records.append((
            apache_match.group('timestamp'),
            m.group('service'),
            1,
            apache_match.group('nbytes')
        ))
        if len(self._service_records) == self.MAX_RAW_RECORDS:
            self.process_raw_service_records()

    def process_raw_service_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        print('processing raw records...')
        columns = ['timestamp', 'service', 'hits', 'nbytes']
        df = pd.DataFrame(self._service_records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=format)
        df['nbytes'] = df['nbytes'].astype(int)

        df = df.groupby([
            df['timestamp'].dt.year,
            df['timestamp'].dt.month,
            df['timestamp'].dt.day,
            df['timestamp'].dt.hour,
            df['service']
        ]).sum()

        df.index.set_names(['year', 'month', 'day', 'hour', 'service'])

        self._intermediate_service_summaries.append(df)

        # Reset
        self._service_records = []

    def finalize_service_records(self):
        """
        """

        # Aggregate the intermediate dataframes into one.
        df = pd.concat(self._intermediate_service_summaries)
        df.index = df.index.set_names(['year', 'month', 'day', 'hour', 'service'])

        df = df.reset_index()

        # Recreate the date column and drop year, month, day, and hour.
        df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
        df = df.drop(labels=['year', 'month', 'day', 'hour'], axis='columns')

        # Re-aggregate over time and service to remove duplicates.
        df = df.groupby(['date', 'service']).sum()
        df = df.reset_index()
        
        # Ok, suitable to send to the database now.
        df.to_sql('observations', self.conn, if_exists='append', index=False)
        self.conn.commit()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = LogProcessor(args.project, args.infile)
    log_processor.run()
