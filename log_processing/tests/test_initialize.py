# standard library imports
import importlib.resources as ir
import json
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd
import psycopg2

# local imports
import arcgis_apache_logs
from arcgis_apache_logs import ApacheLogParser
from .test_core import MockRequestsResponse

class TestSuite(unittest.TestCase):

    def setUp(self):
        """
        Get an independent connection to the database and initialize it.
        """

        self.dbname = 'agpgtest'
        self.conn = psycopg2.connect(dbname=self.dbname)

        with self.conn.cursor() as cursor:
            for schema in ('idpgis', 'nowcoast'):
                cursor.execute(f'drop schema {schema} cascade')

                commands = ir.read_text(arcgis_apache_logs.sql,
                                        f"init_{schema}.sql")
                cursor.execute(commands)

        self._responses = []

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute('drop schema idpgis cascade')
            cursor.execute('drop schema nowcoast cascade')

        self.requests_patcher.stop()

    def _set_response(self, json_response=None):
        if json_response is not None:
            content = json.dumps(json_response).encode('utf-8')
            self._responses.append({'content': content})
            
    def _start_patchers(self):
        
        side_effect = [
            MockRequestsResponse(**kwargs) for kwargs in self._responses
        ]

        patchee = 'arcgis_apache_logs.parse_apache_logs.requests.get'
        self.requests_patcher = patch(patchee, side_effect=side_effect)
        self.requests_patcher.start()

    def test_retrieve_services_from_nco(self):

        json_folders = {
            "folders": [
             "NWS_Forecasts_Guidance_Warnings",
            ],
            "services": []
        }
        self._set_response(json_response=json_folders)

        json_services = {
            "folders": [],
            "services": [{
                "name": "NWS_Forecasts_Guidance_Warnings/NDFD_temp",
                "type": "MapServer"
            },
            {
                "name": "NWS_Forecasts_Guidance_Warnings/ndgd_apm25_hr01_bc",
                "type": "ImageServer"
            }]
        }
        self._set_response(json_response=json_services)

        self._start_patchers()

        with ApacheLogParser('idpgis', dbname=self.dbname) as p:
            actual = p.retrieve_services_from_nco()

        records = [
            {
                'folder': 'NWS_Forecasts_Guidance_Warnings',
                'service': 'NDFD_temp',
                'service_type': 'MapServer',
            },
            {
                'folder': 'NWS_Forecasts_Guidance_Warnings',
                'service': 'ndgd_apm25_hr01_bc',
                'service_type': 'ImageServer',
            },
        ]
        expected = pd.DataFrame(records)

        pd.testing.assert_frame_equal(actual, expected)


