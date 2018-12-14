# Standard library imports ...
import collections
from dataclasses import dataclass
import datetime as dt
import http.client as httplib
import json
import pathlib
import pickle
import re
import sqlite3
import time
import urllib.parse
import urllib.request
import uuid

# Third party library imports ...
import matplotlib as mpl
mpl.use('Agg')
import pandas as pd
import requests
import yaml


@dataclass
class ToolsBase(object):
    """
    Base class for many tools.

    Attributes
    ----------
    site, project : str
        site should be either 'bldr' or 'cprk.
        project should be one of
            nowcoast
            nowcoastqa
            ncesridev
            idpgis
            idpgisqa
            idpgisdev
    conn, cursor : sqlite3 connection objects
        Use these for database connectivity.
    """
    project: str
    site: str

    def __post_init__(self):
        self.ags_port = 6080

        self.db_path = pathlib.Path.home() / 'data' / 'sqlite' / 'gis.db'
        self.conn = sqlite3.connect(str(self.db_path),
                                    detect_types=sqlite3.PARSE_COLNAMES)
        # we want name-based access
        self.conn.row_factory = sqlite3.Row

        self.cursor = self.conn.cursor()

        self._get_ags_credentials()

    def _get_ags_credentials(self):
        """
        Read the arcgis admin credentials from a private file.
        """
        path = pathlib.Path.home() / '.config' / 'arcgis_admin' / 'config.yml'
        if not path.exists():
            msg = (
                f"The configuration file storing AGS credentials "
                f"- {path} - does not exist.  Please create it."
            )
            raise RuntimeError(msg)

        with path.open(mode='rt') as f:
            config = yaml.load(f)
        
        self.username = config['username']
        self.password = config['password']

    def get_token_requests(self, servername):
        """
        Get an AGS token
        """
        url = (f'http://{servername}:{self.server_port}'
               f'/arcgis/admin/generateToken')

        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain",
        }

        params = {
            'username': self.username,
            'password': self.password,
            'client': 'requestip',
            'f': 'json',
        }

        r = requests.post(url, params=params, headers=headers)
        return r.json()['token']

    def get_token(self, servername):
        """
        Authentication requires a token.
        """
        tokenURL = "/arcgis/admin/generateToken"

        # URL-encode the token parameters
        d = {
            'username': self.username,
            'password': self.password,
            'client': 'requestip',
            'f': 'json'
        }
        params = urllib.parse.urlencode(d)

        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain"
        }

        # Connect to URL and post parameters
        httpConn = httplib.HTTPConnection(servername, self.ags_port)
        httpConn.request("POST", tokenURL, params, headers)

        # Read response
        response = httpConn.getresponse()
        if (response.status != 200):
            httpConn.close()
            msg = ("Error while fetching tokens from the admin URL.  "
                   "Please check the URL and try again.")
            raise RuntimeError(msg)

        data = response.read()
        httpConn.close()

        # Check that data returned is not an error object
        self.assertJsonSuccess(data)

        # Extract the token from it
        token = json.loads(data)
        return(token['token'])

    def assertJsonSuccess(self, data):
        """
        A function that checks that the input JSON object
        is not an error object.
        """
        obj = json.loads(data)
        if 'status' in obj and obj['status'] == "error":
            msg = "Error: JSON object returns an error. " + str(obj)
            raise RuntimeError(msg)


@dataclass
class CollectAgsUsageRequests(ToolsBase):
    """
    Collect requests for services over a specific time frame.

    Example:
        obj = CollectAgsUsageRequests('nowcoast', 'bldr', 'op',
                                      '2018-09-20 00:00:00', 24, 'output.pkl')
         
    """
    tier : str
    start_time: dt.datetime
    num_hours: int
    output: str

    def __post_init__(self):
        """
        """
        super().__post_init__()

    def run(self):

        site = self.site if self.site.lower() == 'bldr' else 'lnx'
        project = 'nc' if self.project == 'nowcoast' else ''

        pattern = f"vm-{site}-{project}gisapp-{self.tier}.*"

        # How it would be done in psycopg2.
        # sql = f'''
        #     SELECT * from servers where hostname ~ '{pattern}'
        # '''
        def regexp(pattern, input):
            return bool(re.match(pattern, input))

        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            conn.create_function('regexp', 2, regexp)
            cursor = conn.cursor()

            sql = '''
                  SELECT * from servers
                  where hostname regexp :pattern
                  order by hostname
                  '''
            cursor.execute(sql, {'pattern': pattern})

            # Setup a dictionary to collect data.
            server_data = collections.OrderedDict()
            for row in cursor.fetchall():
                hostname = row['hostname']
                server_data[hostname] = []

        time_index = [
            self.start_time + dt.timedelta(hours=n)
            for n in range(self.num_hours)
        ]

        for idx, hostname in enumerate(server_data.keys()):

            print(hostname)

            token = self.get_token(hostname)
            services = self.get_services(hostname, token)

            for hour_delta in range(0, self.num_hours):
                start_time = self.start_time + dt.timedelta(hours=hour_delta)

                report = self.create_report(hostname, token, services,
                                            start_time)

                # Count the number of requests for the services in the time
                # frame.
                s_index = []
                totalCount = []
                for item in report['report']['report-data'][0]:
                    idx_item = item['resourceURI'].split('/')[-1].split('.')[0]
                    s_index.append(idx_item)
                    try:
                        totalCount.append(sum(item['data']))
                    except TypeError:
                        totalCount.append(0)

                s = pd.Series(totalCount, index=s_index)

                server_data[hostname].append(s.sum())

        df = pd.DataFrame(server_data, index=time_index)

        # Reset the column names.
        columns = [host.split('.')[0] for host in df.columns]
        df.columns = columns

        with open(self.output, 'wb') as f:
            pickle.dump(df, f)

        return df

    def create_report(self, hostname, token, services, start_time):

        # Construct URL to query the logs
        url = (
            f"http://{hostname}:{self.ags_port}"
            f"/arcgis/admin/usagereports/add"
        )

        # Create unique name for temp report
        report_name = uuid.uuid4().hex

        start_time_ms = int(start_time.timestamp() * 1000)
        stop_time_ms = start_time_ms + 3600 * 1000

        # Create report JSON definition
        stats_definition = {
            'reportname': report_name,
            'since': 'CUSTOM',
            'queries': [{
                'resourceURIs': services,
                'metrics': ['RequestCount']
            }],
            'from': start_time_ms,
            'to': stop_time_ms,
            'metadata': {
                'temp': True,
                'tempTimer': int(time.time() * 1000)
            }
        }

        # create report result
        params = {
            'usagereport': json.dumps(stats_definition),
            'f': 'json',
            'token': token,
        }

        r = requests.post(url, params=params)
        r.raise_for_status()

        # query the result.

        url = (
            f"http://{hostname}:{self.ags_port}"
            f"/arcgis/admin/usagereports/{report_name}/data"
        )
        params = {
            'filter': {'machines': '*'},
            'f': 'json',
            'token': token,
        }

        # r = requests.post(url, params=params)
        # r.raise_for_status()
        # report_data = r.json()
        # pprint.pprint(report_data)

        postdata = urllib.parse.urlencode(params).encode('ascii')
        response = urllib.request.urlopen(url, data=postdata)

        if (response.getcode() != 200):
            response.close()
            raise Exception('Error performing request to {0}'.format(url))

        data = response.read()
        response.close()

        # Deserialize response into Python object
        report_data = json.loads(data)

        # Cleanup (delete) statistics report
        url = (
            f"http://{hostname}:{self.ags_port}"
            f"/arcgis/admin/usagereports/{report_name}/delete"
        )
        params = {
            'f': 'json',
            'token': token,
        }
        r = requests.post(url, params=params)
        r.raise_for_status()

        try:
            if report_data['status'] == 'error':
                msg = 'Error retrieving report: {0}'.format(report_data)
                raise RuntimeError(msg)
        except KeyError:
            # 'status' not in the top-level JSON if all was ok.
            pass

        return report_data

    def get_services(self, hostname, token):

        services = []

        root_url = f"http://{hostname}:{self.ags_port}/arcgis/admin/services"
        # print(root_url)

        params = {
            'token': token,
            'f': 'json',
        }
        r = requests.post(root_url, params=params)
        # print(r)
        j = r.json()
        # print(j)

        folders = [folder for folder in j['folders']
                   if folder not in ['System', 'Utilities']]
        for folder_name in folders:

            folder_url = f"{root_url}/{folder_name}"

            r = requests.post(folder_url, params=params)
            j = r.json()
            for service in j['services']:
                services.append((
                    f"services"
                    f"/{folder_name}"
                    f"/{service['serviceName']}.{service['type']}"
                ))

        return services


class CollectAgsStats(ToolsBase):
    """
    Collect ArcGIS server statistics.
    """
    def __init__(self, site, project, priority):
        """
        """
        super().__init__(site, project)

        self.priority = priority
        self.server_port = 6080

    def acquire_stats(self, server, folder, service_name, service_type, token):
        """
        Parameters
        ----------
        server : str
            Host being queried.
        token : str
            AGS token needed for this session.
        """
        url = (f"http://{server}:{self.server_port}"
               f"/arcgis/admin/services/"
               f"{folder}/{service_name}.{service_type}/statistics")

        params = {
            'f': 'json',
            'token': token,
        }
        r = requests.post(url, params=params)
        return r.json()

    def process(self, server, service, j):

        try:
            if j['status'] == 'error':
                return
        except KeyError:
            pass
        # Just use perMachine, ignore summary?  They seem to be the same.
        j = j['perMachine'][0]
        if not j['isStatisticsAvailable']:
            return

        sql = """
              INSERT INTO statistics
              (time, server, service, busy, free, initializing, max,
               notCreated, totalBusyTime, transactions)
              VALUES
              (:time, :server, :service, :busy, :free,
               :initializing, :max,
               :notCreated, :totalBusyTime, :transactions)
              """
        params = {
            'time': dt.datetime.now(),
            'server': server,
            'service': service,
            'busy': j['busy'],
            'free': j['free'],
            'initializing': j['initializing'],
            'max': j['max'],
            'notCreated': j['notCreated'],
            'totalBusyTime': j['totalBusyTime'],
            'transactions': j['transactions'],
        }
        self.cursor.execute(sql, params)

    def acquire_folders(self, server):
        url = f"http://{server}:{self.server_port}/arcgis/rest/services"

        params = {
            'f': 'json',
        }
        r = requests.post(url, params=params)
        return r.json()['folders']

    def acquire_services(self, server, folder):
        url = (f"http://{server}:{self.server_port}"
               f"/arcgis/rest/services/{folder}")

        params = {
            'f': 'json',
        }
        r = requests.post(url, params=params)
        return r.json()['services']

    def run(self):

        now = dt.datetime.now()

        # Clean out old data.
        seven_days_ago = now - dt.timedelta(days=7)
        sql = f"""
               DELETE FROM statistics
               WHERE time < '{seven_days_ago}'
               """
        self.cursor.execute(sql)

        site = 'bldr' if self.site == 'BLDR' else 'lnx'
        project = 'ncgisapp' if self.project == 'nowcoast' else 'gisapp'

        sql = f"""
               SELECT * from servers
               WHERE hostname like 'vm-{site}-{project}%'
               """
        server_df = pd.io.sql.read_sql(sql, self.conn)

        for server_id, row in server_df.iterrows():
            hostname = row['hostname']
            server_id = row['id']
            token = self.get_token(hostname)

            sql = f"""
                   SELECT id, folder, service, service_type, priority
                   FROM services WHERE server_id = {server_id}
                   """
            df_services = pd.io.sql.read_sql(sql, self.conn)

            for idx, service_row in df_services.iterrows():
                # If not a high priority service, then only process if we are
                # at the top of the hour.
                if service_row['priority'] > self.priority:
                    continue

                j = self.acquire_stats(hostname,
                                       service_row['folder'],
                                       service_row['service'],
                                       service_row['service_type'],
                                       token)
                self.process(server_id, service_row['id'], j)

            self.conn.commit()
