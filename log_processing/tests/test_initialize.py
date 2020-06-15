# standard library imports
import importlib.resources as ir
import json
import logging
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd
import psycopg2

# local imports
import arcgis_apache_postgres_logs
from arcgis_apache_postgres_logs import ApacheLogParser
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
                cursor.execute(f'drop schema if exists {schema} cascade')

                commands = ir.read_text(arcgis_apache_postgres_logs.sql,
                                        f"init_{schema}.sql")
                cursor.execute(commands)

        self.conn.commit()
        self._responses = []

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute('drop schema idpgis cascade')
            cursor.execute('drop schema nowcoast cascade')
        self.conn.commit()

        self.requests_patcher.stop()

    def _set_response(self, json_response=None):
        if json_response is not None:
            content = json.dumps(json_response).encode('utf-8')
            self._responses.append({'content': content})

    def _start_patchers(self):

        side_effect = [
            MockRequestsResponse(**kwargs) for kwargs in self._responses
        ]

        patchee = 'arcgis_apache_postgres_logs.parse_apache_logs.requests.get'
        self.requests_patcher = patch(patchee, side_effect=side_effect)
        self.requests_patcher.start()

    def _set_minimal_requests_responses(self):

        json_folders = {
            "folders": [
             "NWS_Forecasts_Guidance_Warnings",
            ],
            "services": []
        }
        self._set_response(json_response=json_folders)

        json_services = {
            "folders": [],
            "services": [
                {
                    "name": "NWS_Forecasts_Guidance_Warnings/NDFD_temp",
                    "type": "MapServer"
                },
                {
                    "name": "NWS_Forecasts_Guidance_Warnings/ndgd_apm25_hr01_bc",  # noqa : E501
                    "type": "ImageServer"
                }
            ]
        }
        self._set_response(json_response=json_services)

    def test_verify_service_log_date_index(self):

        self._start_patchers()

        sql = """
              select *
              from pg_indexes
              where schemaname = 'idpgis' and tablename='service_logs'
              """
        df = pd.read_sql(sql, self.conn)
        self.assertEqual(len(df), 1)
        index_name = df.iloc[0]['indexname']
        self.assertIn('date', index_name)

    def test_retrieve_services_from_nco(self):

        self._set_minimal_requests_responses()

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

    def test_create_new_services(self):
        """
        SCENARIO:  Folders and services are ingested into an empty database.

        EXPECTED RESULT:  The new folders and services are verified.
        """

        self._set_minimal_requests_responses()

        self._start_patchers()

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

        with ApacheLogParser('idpgis', dbname=self.dbname,
                             verbosity=logging.CRITICAL) as p:
            p.create_any_new_services(expected)

        sql = """
        SELECT
            lu_f.folder,
            lu_s.service,
            lu_s.service_type as service_type
        from idpgis.folder_lut lu_f
            inner join idpgis.service_lut lu_s on lu_f.id = lu_s.folder_id
        """
        actual = pd.read_sql(sql, self.conn)
        pd.testing.assert_frame_equal(actual, expected)
