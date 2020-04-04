# Add the service_type column
# populate it
# examine any NULLs remaining in the service type column
# drop the existing index
# Create the new index

import argparse
import datetime as dt
import pathlib

import psycopg2


def run(project, dbname='arcgis_logs'):

    conn = psycopg2.connect(dbname=dbname)
    cursor = conn.cursor()

    cursor.execute(f'set search_path to {project}')

    date = dt.datetime(2020, 4, 6, 0, 0, 0, tzinfo=dt.timezone.utc)
    print(date)

    sql = """
          DELETE FROM {table}
          WHERE date >= %(date)s
          """
    for table in [
        'summary', 'ip_address_logs', 'service_logs', 'referer_logs',
        'user_agent_logs', 'burst'
    ]:
        print(table)
        # df = pd.read_sql(sql2, conn, params=(date,))

        print(sql)
        cursor.execute(sql.format(table=table), {'date': date})
        print(f"{table}:  deleted {cursor.rowcount}")

    conn.commit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('dbname', choices=['arcgis_logs', 'agpgtest', 'tmp_test'])

    args = parser.parse_args()

    run(args.project, args.dbname)
