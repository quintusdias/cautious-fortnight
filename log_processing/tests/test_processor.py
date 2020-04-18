# Standard library imports
import gzip
import importlib.resources as ir
import io
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd
import psycopg2

import arcgis_apache_logs
from arcgis_apache_logs import ApacheLogParser
from .test_core import TestCore


@patch('arcgis_apache_logs.common.logging.getLogger')
class TestSuite(unittest.TestCase):

    def setUp(self):

        self.dbname = 'agpgtest'
        self.conn = psycopg2.connect(dbname=self.dbname)

        with self.conn.cursor() as cursor:
            for schema in ('idpgis', 'nowcoast'):
                cursor.execute(f'drop schema {schema} cascade')

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
                ('radar_base_reflectivity_time', 2, 'ImageServer'::idpgis.svc_type_enum),
                ('ESI_NorthwestArctic_Data', 3, 'MapServer'::idpgis.svc_type_enum),
                ('ESI_Virginia_Data', 3, 'MapServer'::idpgis.svc_type_enum),
                ('wpc_qpf', 1, 'MapServer'::idpgis.svc_type_enum),
                ('radar_base_reflectivity', 4, 'MapServer'::idpgis.svc_type_enum),
                ('NOAA_Estuarine_Bathymetry', 5, 'MapServer'::idpgis.svc_type_enum)
            """
            cursor.execute(sql)

        self.conn.commit()

    def test_good_folder_good_service(self, mock_logger):
        """
        SCENARIO:  The log has one entry that happens to be valid.

        EXPECTED RESULT:   The service_logs, user_agent_logs, referer_logs,
        and ip_address_logs all have one entry.
        """
        with ir.path('tests.data', 'single_valid_entry.gz') as logfile:
            with ApacheLogParser(
                'idpgis', dbname=self.dbname, infile=logfile
            ) as p:
                p.parse_input()

        for table in [
            "service_logs", "user_agent_logs", "referer_logs",
            "ip_address_logs"
        ]:
            sql = f"SELECT * from idpgis.{table}"
            df = pd.read_sql(sql, self.conn)
            self.assertEqual(df.shape[0], 1)

    def test_bad_folder(self, mock_logger):
        """
        SCENARIO:  The input record does not have a recognized folder.

        EXPECTED RESULT:  The service_logs table is still empty.
        """
        with ir.path('tests.data', 'invalid_folder.gz') as logfile:
            with ApacheLogParser(
                'idpgis', dbname=self.dbname, infile=logfile
            ) as p:
                p.parse_input()

        df = pd.read_sql("SELECT * from idpgis.service_logs", self.conn)
        self.assertEqual(df.shape[0], 0)

    def test_export_mapdraw(self, mock_logger):
        """
        SCENARIO:  The record being processed indicates an export mapdraw.

        EXPECTED RESULT:  There is a single record in the service_logs table
        that marks an export map draw, but not a wms map draw.
        """
        b = io.BytesIO()
        log_entry = (
            "2600:387:b:f::bb - - [17/Jul/2019:23:40:31 +0000] "
            "\"GET /idpgis.ncep.noaa.gov.akadns.net/arcgis/rest/services"
            "/NWS_Observations/radar_base_reflectivity/MapServer/export"
            "?bboxSR=3857&imageSR=3857&format=png32&dpi=192&transparent=true"
            "&f=image&size=750.0,1334.0"
            "&bbox=%7B-11229452.36082417%7D,"
            "%0A%7B5920960.861703797%7D,%0A%7B-11228791.814512394%7D,"
            "%0A%7B5922135.75341032%7D&layers=show:0 HTTP/1.1\" " 
            "200 7355 \"-\" \"Hunt/258 CFNetwork/978.0.7 Darwin/18.6.0\" \"-\""
        )
        with gzip.GzipFile(fileobj=b, mode='w') as gf:
            gf.write(log_entry.encode('utf-8'))
        b.seek(0)
        p = ApacheLogParser('idpgis', dbname=self.dbname, infile=b)
        p.parse_input()
        del p

        sql = "SELECT export_mapdraws, wms_mapdraws from idpgis.service_logs"
        df = pd.read_sql(sql, self.conn)

        records = {'export_mapdraws': [1], 'wms_mapdraws': [0]}
        expected = pd.DataFrame(records)
        pd.testing.assert_frame_equal(df, expected)

    def test_wms_mapdraw(self, mock_logger):
        """
        SCENARIO:  The record being processed indicates a wms mapdraw.

        EXPECTED RESULT:  There is a single record in the service_logs table
        that marks a wms map draw, but not an export map draw.
        """
        b = io.BytesIO()
        log_entry = (
            "216.117.49.196 - - [26/Jun/2019:00:03:57 +0000] "
            "\"GET /idpgis.ncep.noaa.gov.akadns.net/arcgis/services"
            "/NWS_Observations/radar_base_reflectivity/MapServer/WmsServer"
            "?SERVICE=WMS&LAYERS=1&CQL_FILTER=INCLUDE&CRS=EPSG:3857"
            "&FORMAT=image%2Fpng&HEIGHT=256&TRANSPARENT=TRUE&REQUEST=GetMap"
            "&WIDTH=256&BBOX=-1.1114555408890909E7,2974317.644632779,"
            "-1.0958012374962868E7,3130860.67856082&STYLES=&VERSION=1.3.0 "
            "HTTP/1.1\" 301 484 \"-\" "
            "\"Jakarta Commons-HttpClient/3.1\" \"-\""
        )
        with gzip.GzipFile(fileobj=b, mode='w') as gf:
            gf.write(log_entry.encode('utf-8'))
        b.seek(0)
        p = ApacheLogParser('idpgis', dbname=self.dbname, infile=b)
        p.parse_input()
        del p

        sql = "SELECT export_mapdraws, wms_mapdraws from idpgis.service_logs"
        df = pd.read_sql(sql, self.conn)

        records = {'export_mapdraws': [0], 'wms_mapdraws': [1]}
        expected = pd.DataFrame(records)
        pd.testing.assert_frame_equal(df, expected)

    @unittest.skip('not yet')
    def test_init_ten_records(self, mock_logger):
        """
        SCENARIO:  Ten IDPGIS log records are processed as the database is
        initialized.  The first two records are in the same hour, but the
        remaining eight are spread through the day.

        EXPECTED RESULT:  There is a single record in the known_referers table,
        as IDP-GIS has no referer support.  There are 5 records in the
        referers_logs table, as records 0 and 1, 3 and 4, and 6 through 9
        get binned.

        Only eight of the ten requests are for defined services, and two of
        those eight are for the same service, 'watch_warn_adv', but both are
        in the same hour.  So the known_services table gets populated with
        seven records, and the services_logs table gets seven records.

        Three IP addresses have two hits each, so the known_ip_addresses table
        has seven entries while the ip_logs table gets eight entries (the first
        two log entries are from the same source during the same hour, same
        for the 4th and 5th).

        Since the referers are all "-", that means that the number of records
        in the summary table match that of the referer_logs table.

        The logger should have been called at the INFO level a few times.
        """
        with ir.path('tests.data', 'ten.dat.gz') as logfile:

            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()
            conn = p.referer.conn

        df = pd.read_sql("SELECT * from known_referers", conn)
        self.assertEqual(df.shape[0], 1)

        df = pd.read_sql("SELECT * from referer_logs", conn)
        self.assertEqual(df.shape[0], 5)

        df = pd.read_sql("SELECT * from known_services", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from service_logs", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from known_ip_addresses", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from ip_address_logs", conn)
        self.assertEqual(df.shape[0], 8)

        df = pd.read_sql("SELECT * from known_user_agents", conn)
        self.assertEqual(df.shape[0], 7)

        df = pd.read_sql("SELECT * from user_agent_logs", conn)
        self.assertEqual(df.shape[0], 8)

        # The data is resampled, so 23 records, one for each hour.
        df = pd.read_sql("SELECT * from summary", conn)
        self.assertEqual(df.shape[0], 23)

        self.assertTrue(p.logger.info.call_count > 0)

    @unittest.skip('not yet')
    def test_records_aggregated(self, mock_logger):
        """
        SCENARIO:  Ten records come in, then the same ten records offset by 30
        minutes come in.

        EXPECTED RESULT:  The tables reflect fully aggregated data.
        """
        with ir.path('tests.data', 'ten.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

            # Get the current number of records in the tables.
            df = pd.read_sql("SELECT * from referer_logs", p.referer.conn)
            n_referer_recs = df.shape[0]
            df = pd.read_sql("SELECT * from service_logs", p.services.conn)
            n_service_recs = df.shape[0]
            df = pd.read_sql("SELECT * from ip_address_logs",
                             p.ip_address.conn)
            n_ip_address_recs = df.shape[0]
            df = pd.read_sql("SELECT * from user_agent_logs",
                             p.user_agent.conn)
            n_user_agent_recs = df.shape[0]

        with ir.path('tests.data', 'another_ten.dat.gz') as logfile:

            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            p.parse_input()

            # There is only 1 new referer record.  All the others fell into
            # existing bins.
            df = pd.read_sql("SELECT * from referer_logs", p.referer.conn)
            self.assertEqual(df.shape[0], n_referer_recs + 1)

            # Out of the ten new records, only 3 new service records  drop into
            # new bins.
            df = pd.read_sql("SELECT * from service_logs", p.services.conn)
            self.assertEqual(df.shape[0], n_service_recs + 3)

            df = pd.read_sql("SELECT * from ip_address_logs",
                             p.ip_address.conn)
            self.assertEqual(df.shape[0], n_ip_address_recs + 3)

            df = pd.read_sql("SELECT * from user_agent_logs",
                             p.user_agent.conn)
            self.assertEqual(df.shape[0], n_user_agent_recs + 3)

    @unittest.skip('not yet')
    def test_deleteme(self, mock_logger):
        """
        SCENARIO:  the request path shows the command was DELETEME, which is
        not a recognized HTTP REST command.

        EXPECTED RESULT:  The error logger is invoked.
        """
        with ir.path('tests.data', 'deleteme.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

        self.assertEqual(p.logger.warning.call_count, 1)

    @unittest.skip('not yet')
    def test_out_of_order(self, mock_logger):
        """
        SCENARIO:  The records are out of order.

        EXPECTED RESULT:  The time series retrieved from the database are in
        order.
        """
        with ir.path('tests.data', 'out_of_order.dat.gz') as logfile:

            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

            p.referer.get_timeseries()

            self.assertTrue(p.referer.df.date.is_monotonic)

    @unittest.skip('not yet')
    def test_puts(self, mock_logger):
        """
        SCENARIO:  The requests are all "PUT"s.

        EXPECTED RESULT:  The results are recorded, not dropped.
        """
        with ir.path('tests.data', 'put.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()
            df = pd.read_sql('select * from referer_logs', p.referer.conn)

        self.assertTrue(len(df) > 0)

    @unittest.skip('not yet')
    def test_co_ops(self, mock_logger):
        """
        SCENARIO:  There are ten records for the 4 CO_OPS FeatureServer and
        MapServer.

        EXPECTED RESULT:  There are hits recorded for the CO_OPS_Stations
        and CO_OPS Products MapServers and FeatureServers.  There are
        2 hits for the CO_OPS Products.
        """
        services = [
            ('NOS_Observations', 'CO_OPS_Stations', 'FeatureServer'),
            ('NOS_Observations', 'CO_OPS_Stations', 'MapServer'),
            ('NOS_Observations', 'CO_OPS_Products', 'FeatureServer'),
            ('NOS_Observations', 'CO_OPS_Products', 'MapServer'),
        ]

        # Put ten records into the database.
        with ir.path('tests.data', 'ten_co_ops.dat.gz') as logfile:
            p1 = ApacheLogParser('idpgis', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p1.services,
                                                 services=services)
            p1.parse_input()

        df = pd.read_sql("SELECT * from service_logs", p1.services.conn)
        df = df.groupby('id').count()

        self.assertEqual(len(df), 4)

    @unittest.skip('not yet')
    def test_baidu(self, mock_logger):
        """
        SCENARIO:  The Baidu referer often manages to get non-UTF8 characters
        into the referer field, which can confuse the file object.
        MapServer.

        EXPECTED RESULT:  The file's single record should be read.
        """
        services = [
            ('nowcoast', 'radar', 'MapServer'),
        ]

        with ir.path('tests.data', 'baidu.dat.gz') as logfile:
            p1 = ApacheLogParser('nowcoast', dbname=self.dbname, infile=logfile)
            self.initialize_known_services_table(p1.services,
                                                 services=services)
            p1.parse_input()

        df = pd.read_sql("SELECT * from referer_logs", p1.referer.conn)
        df = df.groupby('id').count()

        self.assertEqual(len(df), 1)
