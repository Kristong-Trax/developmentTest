
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageobr_sand_template import diageobr_sand_template
from Tests.Data.TestData.test_data_diageobr_sand_sanity import ProjectsSanityData
from Projects.DIAGEOBR_SAND.Calculations import DIAGEOBR_SANDCalculations
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
        connector = ProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @patch('Projects.DIAGEOBR_SAND.Utils.ToolBox.DIAGEOBR_SANDDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-02-20')
    @patch('Projects.DIAGEOBR_SAND.Utils.ToolBox.DIAGEOBR_SANDDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOBR_SAND.Utils.ToolBox.DIAGEOBR_SANDDIAGEOToolBox.download_template',
           return_value=diageobr_sand_template)
    @seeder.seed(["diageobr_sand_seed"], ProjectsSanityData())
    def test_diageobr_sand_sanity(self, x, y, jsom):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['b7f9c0dd-1f63-11e7-af5f-12ea5f726c64']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOBR_SANDCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
