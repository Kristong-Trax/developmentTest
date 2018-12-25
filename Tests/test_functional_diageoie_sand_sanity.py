
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase, skip
from mock import patch

from Tests.Data.Templates.diageoie.LocalMPA import local_mpa
from Tests.Data.Templates.diageoie.MPA import mpa
from Tests.Data.Templates.diageoie.NewProducts import products
from Tests.Data.TestData.test_data_diageoie_sand_sanity import ProjectsSanityData
from Projects.DIAGEOIE_SAND.Calculations import DIAGEOIECalculations
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase


__author__ = 'yoava'
#

class TestKEngineOutOfTheBox(TestMockingFunctionalCase):

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    seeder = Seeder()

    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(0, 0)
        connector.disconnect_rds()

    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-08-02')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.save_latest_templates')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=mpa)
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=local_mpa)
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=products)
    @skip('Test failed in garage')
    @seeder.seed(["diageoie_sand_seed"], ProjectsSanityData())
    def test_diageoie_sand_sanity(self, x, y, json, json2, json3):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['61e77849-2916-4f10-bcfb-94caca30b8a2']
        # for session in sessions:
        #     data_provider.load_session_data(session)
        #     output = Output()
        #     DIAGEOIECalculations(data_provider, output).run_project_calculations()
        #     self._assert_kpi_results_filled()
