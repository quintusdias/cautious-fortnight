#!/usr/bin/env python

import argparse
import datetime as dt
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
    project : str
        Either nowcoast or idpgis
    """
    def __init__(self, project, infile):
        self.project = project
        self.infile = infile

        self.database = pathlib.Path(f'{project}_ips.db')
        if not self.database.exists():
            self.create_database()

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
        self._ip_records = []

    def create_database(self):
        """
        Create an SQLITE database for the IP address records.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        sql = """
              CREATE TABLE observations (
                  date integer,
                  ip_address text,
                  hits integer,
                  errors integer,
                  nbytes integer
              )
              """
        cursor.execute(sql)

    def preprocess_database(self):
        """
        We don't want items in the IP address database getting too old, it will
        take up too much space.
        """
        conn = sqlite3.connect(self.database)
        cursor = conn.cursor()

        # Delete anything too old
        sql = """
              DELETE FROM observations WHERE date < ?
              """
        too_old = dt.datetime.now() - dt.timedelta(days=8)
        cursor.execute(sql, (too_old.timestamp(),))

        conn.commit()

    def run(self):
        self.dbaccess_count = 0
        self.preprocess_database()

        for line in self.infile:
            m = self.apache_regex.match(line)
            if m is None:
                print(line)

            self.process_ip_address(m)

        # Any intermediate processing left to do?
        if len(self._ip_records) > 0:
            self.process_raw_records()

    def process_ip_address(self, apache_match):
        """
        What IP addresses were given?
        """
        timestamp = apache_match.group('timestamp')
        ip_address = apache_match.group('ip_address')
        status_code = int(apache_match.group('status_code'))
        nbytes = int(apache_match.group('nbytes'))

        error = 1 if status_code < 200 or status_code >= 400 else 0

        self._ip_records.append((timestamp, ip_address, 1, error, nbytes))
        if len(self._ip_records) == self.MAX_RAW_RECORDS:
            self.process_raw_records()

    def process_raw_records(self):
        """
        We have reached a limit on how many records we accumulate before
        processing.  Turn what we have into a dataframe and aggregate it
        to the appropriate granularity.
        """
        self.dbaccess_count += 1
        print(f'processing batch of IP address records: {self.dbaccess_count}')  # noqa: E501

        columns = ['timestamp', 'ip_address', 'hits', 'errors', 'nbytes']
        df = pd.DataFrame(self._ip_records, columns=columns)

        format = '%d/%b/%Y:%H:%M:%S %z'
        df['timestamp'] = pd.to_datetime(df['timestamp'], format=format)
        df['nbytes'] = df['nbytes'].astype(int)

        df = df.groupby([
            df['timestamp'].dt.year,
            df['timestamp'].dt.month,
            df['timestamp'].dt.day,
            df['timestamp'].dt.hour,
            df['ip_address']
        ]).sum()

        midx_names = ['year', 'month', 'day', 'hour', 'ip_address']
        df.index = df.index.set_names(midx_names)

        df = df.reset_index()

        # Remake the date into a single column, a timestamp
        df['date'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
        df['date'] = df['date'].astype(np.int64) // 1e9
        df = df.drop(['year', 'month', 'day', 'hour'], axis='columns')

        # Ok, suitable to send to the database now.
        conn = sqlite3.connect(self.database)
        df.to_sql('observations', conn, if_exists='append', index=False)
        conn.commit()

        # Reset
        self._ip_records = []


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = LogProcessor(args.project, args.infile)
    log_processor.run()
