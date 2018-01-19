# Standard library imports
import argparse

from .akamai import RetrieveAkamaiLogs


def get_akamai_logs():
    """
    Retrieve akamai logs from remote FTP server.
    """
    description = ("Command line utility for retrieving Akamai web logs from"
                   "remote FTP server.")
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('project',
                        choices=['idpgis', 'nowcoast'],
                        help='Retrieve logs for this project.')
    args = parser.parse_args()

    obj = RetrieveAkamaiLogs(args.project)
    obj.run()
