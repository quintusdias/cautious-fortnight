# Standard library imports
import importlib.resources as ir
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

from arcgis_apache_logs import ApacheLogParser
from .test_core import TestCore


@patch('arcgis_apache_logs.common.logging.getLogger')
class TestSuite(TestCore):

    def test_bad_folder(self, mock_logger):
        """
        SCENARIO:  Seven IDPGIS log records are processed as the database is
        initialized.  One of the seven records does not come from a recognized
        folder.  The other six records all come from individual services.

        EXPECTED RESULT:  The known_services table is populated with only the
        seven records that it was initially populated with.
        """
        with ir.path('tests.data', 'one_invalid_folder.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

        conn = p.referer.conn

        df = pd.read_sql("SELECT * from known_services", conn)
        self.assertEqual(df.shape[0], 7)

    def test_export_mapdraw(self, mock_logger):
        """
        SCENARIO:  Ten IDPGIS log records are processed, six of them have
        export mapdraws.

        EXPECTED RESULT:  There is a single record in the service_logs table
        that marks a wms map draw.
        """
        with ir.path('tests.data', 'export.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()
            p.services.get_timeseries()
            df = p.services.df

        self.assertEqual(df.loc[1]['export_mapdraws'], 3)
        self.assertEqual(df.loc[2]['export_mapdraws'], 3)
        s = df.sum()
        self.assertEqual(s['export_mapdraws'], 6)

    def test_wms_get_map(self, mock_logger):
        """
        SCENARIO:  Ten IDPGIS log records are processed, one of them has a
        WMS getmap operation.

        EXPECTED RESULT:  There is a single record in the service_logs table
        that marks a wms map draw.
        """
        with ir.path('tests.data', 'ten.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

            p.services.get_timeseries()
            df = p.services.df

        self.assertEqual(df.loc[6]['wms_mapdraws'], 1)
        s = df.sum()
        self.assertEqual(s['wms_mapdraws'], 1)

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

            p = ApacheLogParser('idpgis', logfile)
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

    def test_records_aggregated(self, mock_logger):
        """
        SCENARIO:  Ten records come in, then the same ten records offset by 30
        minutes come in.

        EXPECTED RESULT:  The tables reflect fully aggregated data.
        """
        with ir.path('tests.data', 'ten.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
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

            p = ApacheLogParser('idpgis', logfile)
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

    def test_deleteme(self, mock_logger):
        """
        SCENARIO:  the request path shows the command was DELETEME, which is
        not a recognized HTTP REST command.

        EXPECTED RESULT:  The error logger is invoked.
        """
        with ir.path('tests.data', 'deleteme.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

        self.assertEqual(p.logger.warning.call_count, 1)

    def test_out_of_order(self, mock_logger):
        """
        SCENARIO:  The records are out of order.

        EXPECTED RESULT:  The time series retrieved from the database are in
        order.
        """
        with ir.path('tests.data', 'out_of_order.dat.gz') as logfile:

            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()

            p.referer.get_timeseries()

            self.assertTrue(p.referer.df.date.is_monotonic)

    def test_puts(self, mock_logger):
        """
        SCENARIO:  The requests are all "PUT"s.

        EXPECTED RESULT:  The results are recorded, not dropped.
        """
        with ir.path('tests.data', 'put.dat.gz') as logfile:
            p = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p.services)
            p.parse_input()
            df = pd.read_sql('select * from referer_logs', p.referer.conn)

        self.assertTrue(len(df) > 0)

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
            p1 = ApacheLogParser('idpgis', logfile)
            self.initialize_known_services_table(p1.services,
                                                 services=services)
            p1.parse_input()

        df = pd.read_sql("SELECT * from service_logs", p1.services.conn)
        df = df.groupby('id').count()

        self.assertEqual(len(df), 4)

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
            p1 = ApacheLogParser('nowcoast', logfile)
            self.initialize_known_services_table(p1.services,
                                                 services=services)
            p1.parse_input()

        df = pd.read_sql("SELECT * from referer_logs", p1.referer.conn)
        df = df.groupby('id').count()

        self.assertEqual(len(df), 1)
