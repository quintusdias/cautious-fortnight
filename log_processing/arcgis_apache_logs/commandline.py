# standard library imports
import argparse
import sys

# local imports
from .parse_apache_logs import ApacheLogParser


def parse_arcgis_apache_logs():

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = ApacheLogParser(args.project, args.infile)
    log_processor.run()
