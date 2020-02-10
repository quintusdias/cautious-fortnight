# standard library imports
import argparse

# local imports
from .parse_apache_logs import ApacheLogParser


def parse_arcgis_apache_logs():
    """
    Entry point for parsing the log fragments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('--infile')
    args = parser.parse_args()

    log_processor = ApacheLogParser(args.project, infile=args.infile)
    log_processor.parse_input()


def produce_arcgis_apache_graphics():
    """
    Entry point for creating the HTML and graphics.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    p = ApacheLogParser(args.project, infile=None)
    p.process_graphics()


def initialize_ag_pg_database():
    """
    Entry point for initializing the postgresql database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    args = parser.parse_args()

    processor = ApacheLogParser(args.project)
    processor.initialize_ag_pg_database()


def init_db():
    """
    Entry point for initializing the database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    help = "Initialize the database in this directory."
    parser.add_argument('--document-root', nargs='?', help=help)

    args = parser.parse_args()

    processor = ApacheLogParser(args.project, document_root=args.document_root,
                                infile=None)
    processor.initialize_database()


def prune_arcgis_apache_database():
    """
    Entry point for cleaning up the database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    processor = ApacheLogParser(args.project, infile=None)
    processor.preprocess_database()
