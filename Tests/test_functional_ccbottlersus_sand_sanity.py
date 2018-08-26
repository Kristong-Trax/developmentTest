
import os
import MySQLdb
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase

from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
# from mock import patch

# from Tests.Data.Templates.ccbottlersus.BCI import bci
# from Tests.Data.TestData.test_data_ccbottlersus_sand_sanity import ProjectsSanityData
# from Projects.CCBOTTLERSUS_SAND.Calculations import Calculations
#

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
        connector = ProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    # @patch('Projects.CCBOTTLERSUS_SAND.Utils.ToolBox.BCIToolBox.get_latest_directory_date_from_cloud',
    #        return_value='2018-05-18')
    # @patch('Projects.CCBOTTLERSUS_SAND.Utils.ToolBox.BCIToolBox.save_latest_templates')
    # @patch('Projects.CCBOTTLERSUS_SAND.Utils.ToolBox.BCIToolBox.download_template',
    #        return_value=bci)
    # @seeder.seed(["ccbottlersus_sand_seed"], ProjectsSanityData())
    # def test_ccbottlersus_sand_sanity(self, x, y, json):
    #     project_name = ProjectsSanityData.project_name
    #     data_provider = KEngineDataProvider(project_name)
    #     sessions = ['df86137d-024c-4b4e-8ffa-213dbeefa938']
    #     for session in sessions:
    #         data_provider.load_session_data(session)
    #         output = Output()
    #         Calculations(data_provider, output).run_project_calculations()
    #         self._assert_kpi_results_filled()

    def test_ccbottlersus_sand_sanity(self):
        assert 1 == 1