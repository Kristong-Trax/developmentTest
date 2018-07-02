
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageomx_sand_template import diageomx_sand_template
from Tests.Data.TestData.test_data_diageomx_sand_sanity import ProjectsSanityData
from Projects.DIAGEOMX_SAND.Calculations import DIAGEOMX_SANDCalculations


__author__ = 'yoava'


class TestKEngineOutOfTheBox(MockingTestCase):

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_kpi_results_filled(self):
        connector = ProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @patch('Projects.DIAGEOMX_SAND.Utils.ToolBox.DIAGEOMX_SANDDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('Projects.DIAGEOMX_SAND.Utils.ToolBox.DIAGEOMX_SANDDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOMX_SAND.Utils.ToolBox.DIAGEOMX_SANDDIAGEOToolBox.download_template',
           return_value=diageomx_sand_template)
    @seeder.seed(["diageomx_sand_seed"], ProjectsSanityData())
    def test_diageomx_sand_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['CB67084F-61D5-4D60-9E90-479A0F65C17C']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOMX_SANDCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
