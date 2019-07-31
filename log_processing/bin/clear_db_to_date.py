# Add the service_type column
# populate it
# examine any NULLs remaining in the service type column
# drop the existing index
# Create the new index

import argparse
import datetime as dt
import pathlib
import sqlite3


def run(project, root):

    dbfile = pathlib.Path(root) / f"arcgis_apache_{project}.db"
    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()

    date = dt.datetime(2019, 8, 11, 20, 0, 0).timestamp()
    print(date)

    sql = """
          DELETE FROM {table}
          WHERE date >= ?
          """
    for table in [
        'summary', 'ip_address_logs', 'service_logs', 'referer_logs',
        'user_agent_logs', 'burst_staging'
    ]:
        print(table)
        # df = pd.read_sql(sql2, conn, params=(date,))

        print(sql)
        rs = cursor.execute(sql.format(table=table), (date,))
        print(f"{table}:  deleted {rs.rowcount}")

    conn.commit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('root', help='SQLITE database file parent dir')

    args = parser.parse_args()

    run(args.project, args.root)
