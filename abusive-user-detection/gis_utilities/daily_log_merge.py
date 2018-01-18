"""
"""
import datetime as dt
import pathlib
import subprocess

# Third party libraries
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd


class DailyApacheLogCountPlot(object):
    """
    Plot the running count of the hits from the apache log.
    """
    def __init__(self, project, csv_file):
        self.project = project
        self.csv_file = csv_file

    def run(self):
        """
        Count the number of items in the files
        """
        path = pathlib.Path(f'/mnt/intra_wwwdev/ncep/ncepintradev/htdocs'
                            f'/ncep_common/nowcoast')
        path = path / 'weblogs' / 'nco' / f"{self.project}.png"

        df = pd.read_csv(self.csv_file, parse_dates=[0], index_col='day')
        fig, ax1 = plt.subplots()
        df.plot(ax=ax1)
        fig.savefig(str(path))


class DailyApacheLogCount(object):
    """
    Download and count log file hits for a single day.

    Attributes
    ----------
    project : str
        Specifies the project to process.
    date : datetime.date
        Defines year and month for log file merging.
    logfiles : list
        List of log files for the given month.
    """
    def __init__(self, project, date, dest):
        """
        Parameters
        ----------
        date : datetime.date
            Defines date for daily log file hits counting.
        dest : str
            CSV file
        """
        self.project = project
        self.date = date
        self.dest = dest

    def run(self):
        """
        Count the number of items in the files
        """
        pattern = ('cat '
                   '/home/logs/vm-lnx-idpdm*/httpd/{project}.ncep.noaa.gov'
                   '/access.{year}{month:02d}{day:02}')

        # If we are not running this TODAY, then the log file has not been
        # compressed yet.
        if self.date != dt.date.today():
            pattern += '.gz'

        kwargs = {
            'project': self.project,
            'year': self.date.year,
            'month': self.date.month,
            'day': self.date.day
        }
        command = pattern.format(**kwargs)

        p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        p2 = subprocess.Popen('wc -l', shell=True,
                              stdin=p1.stdout, stdout=subprocess.PIPE)
        p2.wait()
        stdout, stderr = p2.communicate()

        hits = int(stdout.decode('utf-8'))

        with open(self.dest, mode='a') as f:
            print(f"{self.date},{hits}", file=f)
