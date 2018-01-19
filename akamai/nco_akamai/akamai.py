"""
Classes for dealing with Akamai apache logs.
"""

# Standard library imports
from ftplib import FTP
import os
import pathlib


class AkamaiBase(object):
    """
    Base class for Akamai operations.

    Attributes
    ----------
    project : str
        Either 'nowcoast' or 'idpgis'
    local_log_directory, remote_log_directory : str
        Paths to where we retrieve the files and where we put them.
    """

    def __init__(self, project):
        """
        Parameters
        ----------
        project : str
            Either 'nowcoast' or 'idpgis'
        """

        self.project = project

        if self.project == 'nowcoast':
            self.remote_log_directory = 'data'
        else:
            self.remote_log_directory = 'data/idpgis'

        path = pathlib.Path.home()
        path = path / 'data' / 'logs' / 'akamai' / self.project
        self.local_log_directory = path


class RetrieveAkamaiLogs(AkamaiBase):
    """
    Retrieve Akamai apache logs from remote FTP server.

    Attributes
    ----------
    project : str
        Either 'nowcoast' or 'idpgis'
    local_log_directory, remote_log_directory : str
        Paths to where we retrieve the files and where we put them.
    """

    def __init__(self, project):
        """
        Parameters
        ----------
        project : str
            Either 'nowcoast' or 'idpgis'
        """
        super().__init__(project)

    def run(self):
        """
        FTP to the server and retrieve any files not present locally.
        """

        os.chdir(self.local_log_directory / 'incoming')

        ftp = FTP('104.236.112.76')
        ftp.login('akamai', 'sp4nish2ezzentials*')
        ftp.cwd(self.remote_log_directory)

        remote_files = ftp.nlst('*.gz')
        for remote_file in remote_files:
            if not pathlib.Path(remote_file).exists():
                print("retrieving ", remote_file)
                with open(remote_file, 'wb') as localfile:
                    ftp.retrbinary('RETR ' + remote_file,
                                   localfile.write, 1048576)
            else:
                print("skipping ", remote_file)
