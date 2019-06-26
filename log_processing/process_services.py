#!/usr/bin/env python

import argparse
import pathlib
import re
import sqlite3
import sys

# 3rd party library imports
import numpy as np
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

    def create_services_database(self):
        """
        Create an SQLITE database for the service records.
        """
        conn = sqlite3.connect(self.services_database)
        cursor = conn.cursor()

        sql = """
            CREATE TABLE observations (
                date integer,
                folder text,
                service text,
                hits integer,
                errors integer,
                nbytes integer
            )
            """
        cursor.execute(sql)

    def run(self):

        self.dbaccess_count = 0

        for line in self.infile:
            m = self.apache_regex.match(line)
            if m is None:
                print(line)

            self.process_request_path(m)

        # Any intermediate processing left to do?
        if len(self._service_records) > 0:
            self.process_raw_service_records()

    def process_request_path(self, apache_match):
        """
        What services were hit?
        """
        timestamp = apache_match.group('timestamp')
        path = apache_match.group('path')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        m = self.path_regex.match(path)
        if m is None:
            return

        folder = m.group('folder')
        service = m.group('service')

        self._service_records.append((timestamp, folder, service, 1, error, nbytes))
        if len(self._service_records) == self.MAX_RAW_RECORDS:
            self.process_raw_service_records()

    def process_raw_service_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.dbaccess_count += 1
        print(f'processing batch of raw records: {self.dbaccess_count}')  # noqa: E501

        columns = ['timestamp', 'folder', 'service', 'hits', 'errors', 'nbytes']
        df = pd.DataFrame(self._service_records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=format)
        df['nbytes'] = df['nbytes'].astype(int)

        df = df.groupby([
            df['timestamp'].dt.year,
            df['timestamp'].dt.month,
            df['timestamp'].dt.day,
            df['timestamp'].dt.hour,
            df['folder'],
            df['service']
        ]).sum()

        midx_names = ['year', 'month', 'day', 'hour', 'folder', 'service']
        df.index = df.index.set_names(midx_names)

        df = df.reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
        df['date'] = df['date'].astype(np.int64) // 1e9
        df = df.drop(['year', 'month', 'day', 'hour'], axis='columns')

        conn = sqlite3.connect(self.services_database)
        df.to_sql('observations', conn, if_exists='append', index=False)
        conn.commit()

        # Reset
        self._service_records = []


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = LogProcessor(args.project, args.infile)
    log_processor.run()
