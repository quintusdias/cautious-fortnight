# Standard library imports
import datetime as dt
import pathlib
import sqlite3
import sys

# 3rd party library imports
from lxml import etree
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

class ProcessCSV(object):
    """
    Take the CSV file and store it in the SQLITE3 database.

    Attributes
    ----------
    path : path or str
        Path to CSV file.
    df : Dataframe
        Dataframe for current day of referers.
    database_file : path
        Path to SQLITE3 database.
    date : datetime.date
        Date for the CSV file observations.
    """
    def __init__(self, path, datestr):
        """
        Parameters
        ----------
        path : path or str
            Path to CSV file.
        datestr : str
            Eight character string for the date, YYYYMMDD.
        """
        self.path = path
        self.df = pd.read_csv(path)
        self.date = dt.datetime.strptime(datestr, '%Y%m%d')

        self.database_file = pathlib.Path('referer.db')

        if not self.database_file.exists():
            self.create_database()

        self.conn = sqlite3.connect(self.database_file)
        self.cursor = self.conn.cursor()

    def create_database(self):
        """
        Create SQLITE database for the referers.
        """
        self.conn = sqlite3.connect(self.database_file)
        self.cursor = self.conn.cursor()

        # Just a single table.
        sql = """
              CREATE TABLE observations (
                  date integer,
                  referer text,
                  hits integer,
                  hits_403s integer,
                  bytes integer
              )
              """
        self.cursor.execute(sql)

    def run(self):
        # Delete anything older than 10 days.
        sql = """
              DELETE FROM observations WHERE date < ?
              """
        date_10_days_ago = self.date - dt.timedelta(days=30)
        self.cursor.execute(sql, (date_10_days_ago.toordinal(),))

        # Need to create the date column in the dataframe.
        # We will use the proleptic Gregorian ordinal of the date, where
        # January 1 of year 1 has ordinal 1.
        self.df['date'] = self.date.toordinal()
        self.df.columns = ['referer', 'hits', 'hits_403s', 'bytes', 'date']
        self.df.to_sql('observations', self.conn, if_exists='append', index=False)
        self.conn.commit()

if __name__ == '__main__':
    path = pathlib.Path(sys.argv[1])
    datestr = sys.argv[2]
    o = ProcessCSV(path, datestr)
    o.run()
