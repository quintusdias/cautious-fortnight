#!/usr/bin/env python

import argparse
import collections
import operator
import re
import urllib.parse
import sys


class LogProcessor(object):
    """
    Attributes
    ----------
    infile : file-like
        The apache log file (can be stdin).
    apache_regex : object
        Parses lines from the apache log files.
    path_regex : object
        Parses arcgis folders and services from the request path.
    service_hits, service_bytes : dicts
        Count the service hits and bandwidth.
    """
    def __init__(self, infile):
        self.infile = infile

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
            \[(?P<day>\d{2})/(?P<month>\w{3})/(?P<year>\d{4}):
            (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})
            \s(?P<zone>(\+|-)\d{4})\]
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

        self.service_hits = collections.defaultdict(int)
        self.service_nbytes = collections.defaultdict(int)

    def run(self):
        for line in self.infile:
            m = self.apache_regex.match(line)
            if m is None:
                print(line)
            self.process_request_path(m)

        self.summarize_services()

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

        self.service_hits[m.group('service')] += 1
        self.service_nbytes[m.group('service')] += nbytes

    def summarize_services(self):
        sorted_hits = sorted(self.service_hits.items(),
                             key=operator.itemgetter(1),
                             reverse=True)
        for service, num_hits in sorted_hits:
            print(f"{service}  {num_hits} {self.service_nbytes[service]}")
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = LogProcessor(args.infile)
    log_processor.run()

    run(args.infile)
