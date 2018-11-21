
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageomx.MPA import mpa
from Tests.Data.Templates.diageomx.NewProducts import products
from Tests.Data.Templates.diageomx.POSM import posm
from Tests.Data.Templates.diageomx.RelativePosition import position
from Tests.Data.TestData.test_data_diageomx_sanity import ProjectsSanityData
from Projects.DIAGEOMX.Calculations import DIAGEOMXCalculations
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase


__author__ = 'yoava'


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
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.download_template',
           return_value=mpa)
    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.download_template',
           return_value=products)
    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.download_template',
           return_value=position)
    @patch('Projects.DIAGEOMX.Utils.ToolBox.DIAGEOMXDIAGEOToolBox.download_template',
           return_value=posm)
    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_diageomx_sanity(self, x, y, json, json2, json3, json4):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['CB67084F-61D5-4D60-9E90-479A0F65C17C']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOMXCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
