# Add the service_type column
# populate it
# examine any NULLs remaining in the service type column
# drop the existing index
# Create the new index

import argparse

import pandas as pd
import sqlite3


def run(dbfile):

    conn = sqlite3.connect(dbfile)

    cursor = conn.cursor()
    sql = """
          ALTER TABLE summary
          ADD mapdraws INTEGER
          """
    cursor.execute(sql)

    sql = """
          select sql
          from sqlite_master
          where type="table" and name="summary"
          """
    df = pd.read_sql(sql, conn)
    print(df.loc[0]['sql'])

    conn.commit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('dbfile', help='SQLITE database file')

    args = parser.parse_args()

    run(args.dbfile)
