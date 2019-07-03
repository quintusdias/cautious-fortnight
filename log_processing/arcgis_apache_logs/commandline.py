# standard library imports
import argparse
import sys

# local imports
from .parse_apache_logs import ApacheLogParser


def parse_arcgis_apache_logs():

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('--infile', type=argparse.FileType('r'),
                        default=sys.stdin, nargs='?')
    args = parser.parse_args()

    log_processor = ApacheLogParser(args.project, infile=args.infile)
    log_processor.run()


def produce_arcgis_apache_graphics():

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    processor = ApacheLogParser(args.project, infile=None)
    processor.run()
