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

    help = (
        "Write the documents in this directory.  Default is "
        "$HOME/Documents/arcgis_apache_logs"
    )
    parser.add_argument('--document-root', nargs='?', help=help)

    args = parser.parse_args()

    log_processor = ApacheLogParser(args.project, infile=args.infile,
                                    document_root=args.document_root)
    log_processor.run()


def produce_arcgis_apache_graphics():

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    processor = ApacheLogParser(args.project, infile=None)
    processor.run()
