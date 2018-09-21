# Standard library imports
from dataclasses import dataclass
import json

# 3rd party library imports
import pandas as pd
import requests

# Local imports
from .stats import ToolsBase


@dataclass
class AgsRestAdminBase(ToolsBase):
    """
    Access and manipulate ArcGIS server via REST.
    """
    def __post_init__(self):
        super().__post_init__()

        # It would seem all request need to send this.
        self.headers = {
            'Content-type': 'application/x-www-form-urlencoded',
            'Accept': 'text/plain',
        }

        self.config = {
            'cprk': {
                'idpgis': {
                    'op': [
                        'vm-lnx-gisapp-op1a',
                        'vm-lnx-gisapp-op1b',
                        'vm-lnx-gisapp-op2a',
                        'vm-lnx-gisapp-op2b',
                        'vm-lnx-gisapp-op3a',
                        'vm-lnx-gisapp-op3b',
                        'vm-lnx-gisapp-op4a',
                        'vm-lnx-gisapp-op4b',
                        'vm-lnx-gisapp-op5a',
                        'vm-lnx-gisapp-op5b',
                        'vm-lnx-gisapp-op6a',
                        'vm-lnx-gisapp-op6b',
                    ],
                    'qa': [
                        'vm-lnx-gisapp-qa1a',
                        'vm-lnx-gisapp-qa1b',
                        'vm-lnx-gisapp-qa2a',
                        'vm-lnx-gisapp-qa2b',
                        'vm-lnx-gisapp-qa3a',
                        'vm-lnx-gisapp-qa3b',
                        'vm-lnx-gisapp-qa4a',
                        'vm-lnx-gisapp-qa4b',
                        'vm-lnx-gisapp-qa5a',
                        'vm-lnx-gisapp-qa5b',
                        'vm-lnx-gisapp-qa6a',
                        'vm-lnx-gisapp-qa6b',
                    ],
                    'dev': [
                        'vm-lnx-gisapp-dev3',
                        'vm-lnx-gisapp-dev4',
                        'vm-lnx-gisapp-dev5',
                        'vm-lnx-gisapp-dev6',
                        'vm-lnx-gisapp-dev7',
                        'vm-lnx-gisapp-dev8',
                    ],
                },
                'nowcoast': {
                    'op': [
                        'vm-lnx-ncgisapp-op1a',
                        'vm-lnx-ncgisapp-op1b',
                        'vm-lnx-ncgisapp-op2a',
                        'vm-lnx-ncgisapp-op2b',
                        'vm-lnx-ncgisapp-op3a',
                        'vm-lnx-ncgisapp-op3b',
                        'vm-lnx-ncgisapp-op4a',
                        'vm-lnx-ncgisapp-op4b',
                        'vm-lnx-ncgisapp-op5a',
                        'vm-lnx-ncgisapp-op5b',
                        'vm-lnx-ncgisapp-op6a',
                        'vm-lnx-ncgisapp-op6b',
                    ],
                    'qa': [
                        'vm-lnx-ncgisapp-qa1a',
                        'vm-lnx-ncgisapp-qa1b',
                        'vm-lnx-ncgisapp-qa2a',
                        'vm-lnx-ncgisapp-qa2b',
                        'vm-lnx-ncgisapp-qa3a',
                        'vm-lnx-ncgisapp-qa3b',
                        'vm-lnx-ncgisapp-qa4a',
                        'vm-lnx-ncgisapp-qa4b',
                        'vm-lnx-ncgisapp-qa5a',
                        'vm-lnx-ncgisapp-qa5b',
                        'vm-lnx-ncgisapp-qa6a',
                        'vm-lnx-ncgisapp-qa6b',
                    ],
                    'dev': [
                        'vm-lnx-ncgisapp-dev1',
                        'vm-lnx-ncgisapp-dev2',
                        'vm-lnx-ncgisapp-dev3',
                        'vm-lnx-ncgisapp-dev4',
                        'vm-lnx-ncgisapp-dev5',
                        'vm-lnx-ncgisapp-dev6',
                    ],
                },
            },
            'bldr': {
                'idpgis': {
                    'op': [
                        'vm-bldr-gisapp-op1a',
                        'vm-bldr-gisapp-op1b',
                        'vm-bldr-gisapp-op2a',
                        'vm-bldr-gisapp-op2b',
                        'vm-bldr-gisapp-op3a',
                        'vm-bldr-gisapp-op3b',
                        'vm-bldr-gisapp-op4a',
                        'vm-bldr-gisapp-op4b',
                        'vm-bldr-gisapp-op5a',
                        'vm-bldr-gisapp-op5b',
                        'vm-bldr-gisapp-op6a',
                        'vm-bldr-gisapp-op6b',
                    ],
                    'qa': [
                        'vm-bldr-gisapp-qa1a',
                        'vm-bldr-gisapp-qa1b',
                        'vm-bldr-gisapp-qa2a',
                        'vm-bldr-gisapp-qa2b',
                        'vm-bldr-gisapp-qa3a',
                        'vm-bldr-gisapp-qa3b',
                        'vm-bldr-gisapp-qa4a',
                        'vm-bldr-gisapp-qa4b',
                        'vm-bldr-gisapp-qa5a',
                        'vm-bldr-gisapp-qa5b',
                        'vm-bldr-gisapp-qa6a',
                        'vm-bldr-gisapp-qa6b',
                    ],
                    'dev': [
                        'vm-bldr-gisapp-dev1a',
                        'vm-bldr-gisapp-dev1b',
                        'vm-bldr-gisapp-dev2a',
                        'vm-bldr-gisapp-dev2b',
                        'vm-bldr-gisapp-dev3a',
                        'vm-bldr-gisapp-dev3b',
                        'vm-bldr-gisapp-dev4a',
                        'vm-bldr-gisapp-dev4b',
                        'vm-bldr-gisapp-dev5a',
                        'vm-bldr-gisapp-dev5b',
                        'vm-bldr-gisapp-dev6a',
                        'vm-bldr-gisapp-dev6b',
                    ],
                },
                'nowcoast': {
                    'op': [
                        'vm-bldr-ncgisapp-op1a',
                        'vm-bldr-ncgisapp-op1b',
                        'vm-bldr-ncgisapp-op2a',
                        'vm-bldr-ncgisapp-op2b',
                        'vm-bldr-ncgisapp-op3a',
                        'vm-bldr-ncgisapp-op3b',
                        'vm-bldr-ncgisapp-op4a',
                        'vm-bldr-ncgisapp-op4b',
                        'vm-bldr-ncgisapp-op5a',
                        'vm-bldr-ncgisapp-op5b',
                        'vm-bldr-ncgisapp-op6a',
                        'vm-bldr-ncgisapp-op6b',
                    ],
                    'qa': [
                        'vm-bldr-ncgisapp-qa1a',
                        'vm-bldr-ncgisapp-qa1b',
                        'vm-bldr-ncgisapp-qa2a',
                        'vm-bldr-ncgisapp-qa2b',
                        'vm-bldr-ncgisapp-qa3a',
                        'vm-bldr-ncgisapp-qa3b',
                        'vm-bldr-ncgisapp-qa4a',
                        'vm-bldr-ncgisapp-qa4b',
                        'vm-bldr-ncgisapp-qa5a',
                        'vm-bldr-ncgisapp-qa5b',
                        'vm-bldr-ncgisapp-qa6a',
                        'vm-bldr-ncgisapp-qa6b',
                    ],
                    'dev': [
                        'vm-bldr-ncgisapp-dev1a',
                        'vm-bldr-ncgisapp-dev1b',
                        'vm-bldr-ncgisapp-dev2a',
                        'vm-bldr-ncgisapp-dev2b',
                        'vm-bldr-ncgisapp-dev3a',
                        'vm-bldr-ncgisapp-dev3b',
                        'vm-bldr-ncgisapp-dev4a',
                        'vm-bldr-ncgisapp-dev4b',
                        'vm-bldr-ncgisapp-dev5a',
                        'vm-bldr-ncgisapp-dev5b',
                        'vm-bldr-ncgisapp-dev6a',
                        'vm-bldr-ncgisapp-dev6b',
                    ],
                },
            },
        }


class AgsRestAdmin(AgsRestAdminBase):
    """
    Access and manipulate ArcGIS server via REST.

    Attributes
    ----------
    site, project, tier : str
        Something like 'BLDR' 'nowcoast' 'dev'
    server, service : str
        If provided, limit the operation to this server and/or service
    parameter, value : str
        Change this parameter to the given value.  If value is not provided,
        just query the current value.
    headers : dict
        These header key/value pairs must be sent with each request.
    """
    def __init__(self, site, project, tier, parameter, value=None, server=None,
                 service=None):
        super().__init__(site, project)

        self.tier = tier
        self.parameter = parameter
        self.value = value
        self.server = server
        self.service = service
        self.s = requests.Session()

    def get_logs(self, server,
                 folder='nowcoast', service=None, service_type='MapServer',
                 level='WARNING'):
        """
        Returns
        -------
        dataframe of log messages
        """
        self.token = self.get_token(server)

        log_filter = {'services': [f'{folder}/{service}.{service_type}']}

        # Supply the log level, filter, token, and return format
        params = {
            'level': level,
            'filter': str(log_filter),
            'token': self.token,
            'f': 'json',
        }

        url = f"http://{server}:6080/arcgis/admin/logs/query"
        r = self.s.post(url, params=params, headers=self.headers)
        r.raise_for_status()

        data = r.json()

        df = pd.DataFrame(data['logMessages'])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df

    def get_services(self, server):
        """
        Return list of services for this server
        """
        url = f'http://{server}:6080/arcgis/rest'
        params = {'f': 'pjson'}
        r = requests.get(url, params=params, verify=False)
        r.raise_for_status()

        # Not usually the case, but maybe there's a service in the root
        # folder?
        services = []
        for item in r.json()['services']:
            services.append(f"{item['name']}.{item['type']}")

        # Ok now go thru the folders.
        for folder in r.json()['folders']:
            url = f'http://{server}:6080/arcgis/rest/services/{folder}'
            rfolder = requests.get(url, params=params, verify=False)
            for item in rfolder.json()['services']:
                services.append(f"{item['name']}.{item['type']}")

        return services

    def set_log_parameter(self, value):

        for server in self.config[self.project][self.site][self.tier]:

            if self.server is not None and not self.server.startswith(server):
                print(f'Skipping {server}...')
                continue

            server = server + '.ncep.noaa.gov'
            self.token = self.get_token(server)

            params = {'token': self.token, 'f': 'json'}
            url = f"http://{server}:{self.ags_port}/arcgis/admin/logs/settings"
            r = self.s.post(url, params=params, headers=self.headers)
            r.raise_for_status()

            settings = r.json()
            old_value = settings['settings'][self.parameter]
            print(f"{server}:  {old_value}", end='')

            if value is None:
                print()
                continue

            print(f" ==> {value}")

            # Change the log settings.
            url = (f"http://{server}:{self.ags_port}"
                   f"/arcgis/admin/logs/settings/edit")

            params = {'token': self.token, 'f': 'json'}
            try:
                params[self.parameter] = int(value)
            except ValueError:
                params[self.parameter] = value

            # for certain parameters, keep as they are
            keepers = set(['logDir', 'maxLogFileAge', 'maxErrorReportsCount'])
            keepers = keepers - set([self.parameter])
            for parameter in keepers:
                params[parameter] = settings['settings'][parameter]

            r = self.s.post(url, params=params, headers=self.headers)
            r.raise_for_status()

            settings = r.json()

            if settings['status'] != 'success':
                msg = ("Error while updating from the admin URL.  "
                       "Please check the URL and try again.")
                raise RuntimeError(msg)

    def set_status(self, value):
        """
        Command the service to either start, stop, or restart.
        """

        if value in ['start', 'stop']:
            self.start_or_stop(value)
            return

        if value == 'restart':
            self.start_or_stop('stop')
            self.start_or_stop('start')
            return

        for server in self.config[self.site][self.project][self.tier]:

            if self.server is not None and not self.server.startswith(server):
                print(f'Skipping {server}...')
                continue

            server = server + '.ncep.noaa.gov'
            print(f'Looking at {server}...')
            self.token = self.get_token(server)

            services = self.get_services(server)
            for service in services:

                if self.service is not None:
                    # If the service is specified, then only do that service.
                    if service != self.service:
                        continue

                # Ok are we just reporting the status?
                # Get the service information
                url = f'/arcgis/admin/services/{service}'
                params = {'token': self.token, 'f': 'json'}

                url = (f"http://{server}:{self.ags_port}"
                       f"/arcgis/admin/services/{service}")
                r = self.s.post(url, params=params, headers=self.headers)
                r.raise_for_status()

                settings = r.json()

                # pprint.pprint(settings)
                print(f"{service}: {settings['configuredState']}")

    def set_parameter(self, value):
        if self.parameter in ['logLevel', 'maxLogFileAge']:
            self.set_log_parameter(value)
            return

        if self.parameter == "status":
            self.set_status(value)
            return

        for server in self.config[self.site][self.project][self.tier]:

            if self.server is not None and not self.server.startswith(server):
                print(f'Skipping {server}...')
                continue

            server = server + '.ncep.noaa.gov'
            print(f'Looking at {server}...')
            self.token = self.get_token(server)

            services = self.get_services(server)
            for service in services:

                if 'FeatureServer' in service:
                    # Can we not do this?
                    print(f'Skipping {server}...')
                    continue

                if self.service is not None:
                    # If the service is specified, then only do that service.
                    if service != self.service:
                        continue

                # Get the service information
                params = {'token': self.token, 'f': 'json'}

                url = f'http://{server}:6080/arcgis/admin/services/{service}'
                r = self.s.post(url, params=params, headers=self.headers)
                r.raise_for_status()

                # settings = json.loads(data)
                settings = r.json()

                if self.parameter == 'enableDynamicLayers':
                    if settings['type'] != 'MapServer':
                        # Only MapServer has enableDynamicLayers ??
                        continue
                    print((f"{service}: "
                           f"{settings['properties'][f'{self.parameter}']}"))
                else:
                    print(f"{service}: {settings[f'{self.parameter}']}")
                if value is None:
                    continue

                if self.parameter in ['maxStartupTime', 'minInstancesPerNode']:
                    # For these two parameters only, the value must be integer.
                    value = int(value)

                # Is the value already set?
                if settings[self.parameter] == value:
                    print('Already set...')
                    continue

                settings[self.parameter] = value

                # Serialize back to JSON
                updatedSvcJson = json.dumps(settings)

                url = (f'http://{server}:6080'
                       f'/arcgis/admin/services/{service}/edit')
                params = {
                    'token': self.token,
                    'f': 'json',
                    'service': updatedSvcJson
                }

                r = self.s.post(url, params=params, headers=self.headers)
                r.raise_for_status()
                pass

    def start_or_stop(self, command):
        """
        Parameters
        ----------
        command : str
            Either "start" or "stop"
        """
        for server in self.config[self.site][self.project][self.tier]:

            if self.server is not None and not self.server.startswith(server):
                print(f'Skipping {server}...')
                continue

            server = server + '.ncep.noaa.gov'
            print(f'Looking at {server}...')
            self.token = self.get_token(server)

            service = self.service

            print(f"Trying to {command} {service}...")

            url = (f'http://{server}:6080'
                   f'/arcgis/admin/services/{service}/{command}')
            params = {'token': self.token, 'f': 'json'}

            r = self.s.post(url, params=params, headers=self.headers)
            r.raise_for_status()

            obj = r.json()
            if 'status' in obj and obj['status'] == "error":
                msg = (f"AGS reports error while trying to {command} "
                       f"the service.")
                raise RuntimeError(msg)

            print(f"{service} has been commanded to {command}...")
