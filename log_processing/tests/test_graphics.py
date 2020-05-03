# standard library imports
import datetime as dt
import importlib.resources as ir
import unittest

# 3rd party library imports
import psycopg2

# local imports
import arcgis_apache_logs
from arcgis_apache_logs import ApacheLogParser


class TestSuite(unittest.TestCase):

    def setUp(self):

        self.dbname = 'agpgtest'
        self.conn = psycopg2.connect(dbname=self.dbname)

        with self.conn.cursor() as cursor:
            for schema in ('idpgis', 'nowcoast'):
                cursor.execute(f'drop schema if exists {schema} cascade')

                commands = ir.read_text(arcgis_apache_logs.sql,
                                        f"init_{schema}.sql")
                cursor.execute(commands)

            sql = """
            insert into idpgis.folder_lut (folder)
            values
                ('NWS_Forecasts_Guidance_Warnings'),
                ('radar'),
                ('NOS_ESI'),
                ('NWS_Observations'),
                ('NOAA')
            """
            cursor.execute(sql)

            sql = """
            insert into idpgis.service_lut
            (service, folder_id, service_type)
            values
                ('watch_warn_adv', 1, 'MapServer'::idpgis.svc_type_enum),
                (
                    'radar_base_reflectivity_time',
                    2,
                    'ImageServer'::idpgis.svc_type_enum
                ),
                (
                    'ESI_NorthwestArctic_Data',
                    3,
                    'MapServer'::idpgis.svc_type_enum
                ),
                ('ESI_Virginia_Data', 3, 'MapServer'::idpgis.svc_type_enum),
                ('wpc_qpf', 1, 'MapServer'::idpgis.svc_type_enum),
                (
                    'radar_base_reflectivity',
                    4,
                    'MapServer'::idpgis.svc_type_enum
                ),
                (
                    'NOAA_Estuarine_Bathymetry',
                    5,
                    'MapServer'::idpgis.svc_type_enum
                )
            """
            cursor.execute(sql)

        self.conn.commit()

    def tearDown(self):
        with self.conn.cursor() as cursor:
            cursor.execute('drop schema idpgis cascade')
            cursor.execute('drop schema nowcoast cascade')

    def test_smoke(self):
        """
        SCENARIO:  the graphics processing is run

        EXPECTED RESULT:  no errors
        """

        import io
        import gzip
        with ir.path('tests.data', 'ten.dat.gz') as logfile:

            # Replace the fixed (and aging ) timestamps with yesterday,
            # otherwise the graphics won't work with it.
            with gzip.open(logfile) as f:
                yesterday = dt.date.today() - dt.timedelta(days=1)
                s = f.read().decode('utf-8')
                s = s.replace('25/Jun/2019', yesterday.strftime('%d/%b/%Y'))

            b = io.BytesIO()
            with gzip.GzipFile(fileobj=b, mode='w') as gf:
                gf.write(s.encode('utf-8'))
            b.seek(0)

            with ApacheLogParser('idpgis', dbname=self.dbname, infile=b) as p:
                p.parse_input()

        with ApacheLogParser('idpgis', dbname=self.dbname) as p:
            p.process_graphics()

