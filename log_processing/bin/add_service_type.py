# Add the service_type column
# populate it
# examine any NULLs remaining in the service type column
# drop the existing index
# Create the new index

import argparse

import pandas as pd
import requests
import sqlite3


def add_service_type_column(conn):
    cursor = conn.cursor()
    sql = """
          ALTER TABLE known_services
          ADD service_type TEXT
          """
    cursor.execute(sql)
    conn.commit()


def get_services(project):
    url = f"https://{project}.ncep.noaa.gov/arcgis/rest/services"
    params = {'f': 'json'}
    r = requests.get(url, params=params)
    j = r.json()
    folders = j['folders']

    services = []
    for folder in folders:
        url = f"https://{project}.ncep.noaa.gov/arcgis/rest/services/{folder}"
        r = requests.get(url, params=params)
        j = r.json()
        for item in j['services']:
            services.append(f"{item['name']}/{item['type']}")
    return services


def update_known_services(conn, services):
    cursor = conn.cursor()
    for item in services:
        print(item)
        folder, service, svc_type = item.split('/')
        sql = """
              UPDATE known_services
              SET service_type = ?
              WHERE folder = ? AND service = ?
              """
        cursor.execute(sql, (svc_type, folder, service))
    conn.commit()


def recreate_indices(conn):
    cursor = conn.cursor()
    sql = """
          DROP INDEX idx_services
          """
    cursor.execute(sql)

    sql = """
          CREATE UNIQUE INDEX idx_services
          ON known_services(folder, service, service_type)
          """
    cursor.execute(sql)
    conn.commit()
    print('done?')


def check_table_nulls(conn):

    sql = """
          SELECT id from known_services WHERE service_type IS NULL
          """
    df = pd.read_sql(sql, conn)
    print(df)

    cursor = conn.cursor()
    sql = f"""
           DELETE FROM service_logs
           WHERE id IN {tuple(df.id.values)}
           """
    print(sql)
    rs = cursor.execute(sql)
    print(f'deleted {rs.rowcount}')
    conn.commit()

    sql = """
          DELETE from known_services WHERE service_type IS NULL
          """
    print(sql)
    cursor = conn.cursor()
    rs = cursor.execute(sql)
    print(f'deleted {rs.rowcount}')
    conn.commit()


def run(project, dbfile):

    conn = sqlite3.connect(dbfile)
    add_service_type_column(conn)
    services = get_services(project)
    update_known_services(conn, services)
    check_table_nulls(conn)
    recreate_indices(conn)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('dbfile', help='SQLITE database file')

    args = parser.parse_args()

    run(args.project, args.dbfile)
