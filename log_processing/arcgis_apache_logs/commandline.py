# standard library imports
import argparse
import sys

# local imports
from .parse_apache_logs import ApacheLogParser


def parse_arcgis_apache_logs():
    """
    Entry point for parsing the log fragments.
    """

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
    log_processor.parse_input()


def produce_arcgis_apache_graphics():
    """
    Entry point for creating the HTML and graphics.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    p = ApacheLogParser(args.project, infile=None)
    p.produce_graphics()


def prune_arcgis_apache_database():
    """
    Entry point for cleaning up the database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    processor = ApacheLogParser(args.project, infile=None)
    processor.preprocess_database()
