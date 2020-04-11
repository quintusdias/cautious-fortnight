# standard library imports
import argparse

# local imports
from .parse_apache_logs import ApacheLogParser


def parse_ags_logs():
    """
    Entry point for parsing the log fragments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('--infile')

    help = "Database name"
    parser.add_argument('--dbname', default='arcgis_logs', help=help)

    args = parser.parse_args()

    log_processor = ApacheLogParser(args.project, infile=args.infile,
                                    dbname=args.dbname)
    log_processor.parse_input()


def produce_ags_graphics():
    """
    Entry point for creating the HTML and graphics.
    """

    parser = argparse.ArgumentParser()

    help = "Database name"
    parser.add_argument('--dbname', default='arcgis_logs', help=help)

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    args = parser.parse_args()

    p = ApacheLogParser(args.project, infile=None, dbname=args.dbname)
    p.process_graphics()


def initialize_ags_database():
    """
    Entry point for initializing the postgresql database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    args = parser.parse_args()

    processor = ApacheLogParser(args.project, init=True)
    processor.initialize_ags_database()


def check_ags_services():
    """
    Entry point for checking the database against existing services without
    updating.  This is useful in case you want to see beforehand what would
    happen.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    help = "Database name"
    parser.add_argument('--dbname', default='arcgis_logs', help=help)

    args = parser.parse_args()

    processor = ApacheLogParser(args.project, dbname=args.dbname)
    processor.check_ags_services()


def update_ags_database():
    """
    Entry point for updating the postgresql database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    help = "Database name"
    parser.add_argument('--dbname', default='arcgis_logs', help=help)

    args = parser.parse_args()

    processor = ApacheLogParser(args.project, dbname=args.dbname)
    processor.update_ags_services()


def prune_ags_database():
    """
    Entry point for cleaning up the database.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('project', choices=['idpgis', 'nowcoast'])

    help = "Database name"
    parser.add_argument('--dbname', default='arcgis_logs', help=help)

    args = parser.parse_args()

    processor = ApacheLogParser(args.project, dbname=args.dbname)
    processor.preprocess_database()
