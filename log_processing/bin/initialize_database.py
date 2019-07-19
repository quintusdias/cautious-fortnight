# Add the service_type column
# populate it
# examine any NULLs remaining in the service type column
# drop the existing index
# Create the new index

import argparse

import pandas as pd
import requests

from arcgis_apache_logs import ApacheLogParser


def get_services(project):
    url = f"https://{project}.ncep.noaa.gov/arcgis/rest/services"
    params = {'f': 'json'}
    r = requests.get(url, params=params)
    j = r.json()
    folders = j['folders']

    records = []
    for folder in folders:
        url = f"https://{project}.ncep.noaa.gov/arcgis/rest/services/{folder}"
        r = requests.get(url, params=params)
        j = r.json()
        for item in j['services']:
            folder, service = item['name'].split('/')
            service_type = item['type']
            records.append((folder, service, service_type))

    columns = ['folder', 'service', 'service_type']
    df = pd.DataFrame.from_records(records, columns=columns)
    return df


def run(project, root):

    p = ApacheLogParser(project, document_root=root)

    df = get_services(project)
    df.to_sql('known_services', p.services.conn,
              index=False, if_exists='append')
    p.services.conn.commit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('project', choices=['idpgis', 'nowcoast'])
    parser.add_argument('root', help='SQLITE database file parent dir')

    args = parser.parse_args()

    run(args.project, args.root)
