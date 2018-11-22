
import os
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageoga_sand_template import diageoga_sand_template
from Tests.Data.TestData.test_data_diageoga_sand_sanity import ProjectsSanityData
from Projects.DIAGEOGA_SAND.Calculations import DIAGEOGACalculations
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

    @patch('Projects.DIAGEOGA_SAND.Utils.ToolBox.DIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-02-20')
    @patch('Projects.DIAGEOGA_SAND.Utils.ToolBox.DIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOGA_SAND.Utils.ToolBox.DIAGEOToolBox.download_template',
           return_value=diageoga_sand_template)
    @seeder.seed(["diageoga_sand_seed"], ProjectsSanityData())
    def test_diageoga_sand_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['11de22ce-2056-4064-9c78-21fce74bb764']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOGACalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
