# Standard library imports
import argparse
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
    def __init__(self, path):
        """
        Parameters
        ----------
        path : path or str
            Path to CSV file.
        """
        self.path = path

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
                  errors integer,
                  bytes integer
              )
              """
        self.cursor.execute(sql)

    def preprocess_database(self):

        # Delete anything older than 10 days.
        sql = """
              DELETE FROM observations WHERE date < ?
              """
        too_old = dt.datetime.now() - dt.timedelta(days=30)
        self.cursor.execute(sql, (too_old.timestamp(),))

        # Delete anything newer than the old observation in the dataframe.
        sql = """
              DELETE FROM observations WHERE date >= ?
              """
        too_new = self.df['date'].min()
        self.cursor.execute(sql, (too_new,))

    def load_referer_data(self):

        self.df = pd.read_csv(self.path, parse_dates=[0])
        self.df.columns = ['date', 'referer', 'hits', 'errors', 'bytes']

        # The hits and error columns might have NaNs in them.
        self.df['hits'] = self.df['hits'].fillna(value=0)
        self.df['errors'] = self.df['errors'].fillna(value=0)

        # Turn the date column into unix timestamps.
        self.df['date'] = (self.df['date'] - pd.Timestamp('1970-01-01')) // pd.Timedelta('1s')

    def run(self):

        self.load_referer_data()
        self.preprocess_database()
        self.df.to_sql('observations', self.conn, if_exists='append', index=False)
        self.conn.commit()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('csvfile', help="CSV file")
    args = parser.parse_args()

    o = ProcessCSV(args.csvfile)
    o.run()
