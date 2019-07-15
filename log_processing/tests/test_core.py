# Standard libraary imports
import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

# 3rd party library imports
import pandas as pd

# Local imports


class TestCore(unittest.TestCase):

    def setUp(self):
        """
        Create a temporary directory in which to create artifacts (often the
        current directory).
        """

        self.starting_dir = pathlib.Path.cwd()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)

        os.chdir(self.tempdir.name)

        fake_home_dir = tempfile.TemporaryDirectory()
        self.fake_home_dir = pathlib.Path(fake_home_dir.name)
        self.addCleanup(fake_home_dir.cleanup)

        patchee = 'arcgis_apache_logs.common.pathlib.Path.home'
        self.homedir_patcher = patch(patchee, return_value=self.fake_home_dir)
        self.homedir_patcher.start()

    def tearDown(self):
        """
        Change back to the starting directory and remove any artifacts created
        during a test.
        """
        os.chdir(self.starting_dir)

        self.homedir_patcher.stop()

    def initialize_known_services_table(self, svcs_obj, services=None):
        """
        The known services table must be initialized BEFORE it can be used.
        """
        if services is None:
            services = [
                ('NWS_Forecasts_Guidance_Warnings', 'watch_warn_adv',
                 'MapServer'),
                ('radar', 'radar_base_reflectivity_time', 'ImageServer'),
                ('NOS_ESI', 'ESI_NorthwestArctic_Data', 'MapServer'),
                ('NOS_ESI', 'ESI_Virginia_Data', 'MapServer'),
                ('NWS_Forecasts_Guidance_Warnings', 'wpc_qpf', 'MapServer'),
                ('NWS_Observations', 'radar_base_reflectivity', 'MapServer'),
                ('NOAA', 'NOAA_Estuarine_Bathymetry', 'MapServer'),
            ]
        columns = ['folder', 'service', 'service_type']
        df = pd.DataFrame.from_records(services, columns=columns)
        df.to_sql('known_services', svcs_obj.conn,
                  index=False, if_exists='append')
