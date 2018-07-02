
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.ccus_sand_template import ccus_sand_json
from Tests.Data.TestData.test_data_ccus_sand_sanity import ProjectsSanityData
from Projects.CCUS_SAND.Calculations import CCUSCalculations


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

    @patch('Projects.CCUS_SAND.Utils.ToolBox.ToolBox.get_latest_directory_date_from_cloud', return_value='2018-01-01')
    @patch('Projects.CCUS_SAND.Utils.ToolBox.ToolBox.save_latest_templates')
    @patch('Projects.CCUS_SAND.Utils.ToolBox.ToolBox.download_template', return_value=ccus_sand_json)
    @seeder.seed(["ccus_sand_seed"], ProjectsSanityData())
    def test_ccus_sand_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['327d4acd-25be-4e8b-883c-b3e767f246fa']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            CCUSCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
