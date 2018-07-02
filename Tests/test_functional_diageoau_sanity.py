
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageoau_template import diageoau_template
from Tests.Data.TestData.test_data_diageoau_sanity import ProjectsSanityData
from Projects.DIAGEOAU.Calculations import DIAGEOAUCalculations


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

    @patch('Projects.DIAGEOAU.Utils.ToolBox.DIAGEOAUDIAGEOToolBox.get_latest_directory_date_from_cloud', return_value='2018-01-01')
    @patch('Projects.DIAGEOAU.Utils.ToolBox.DIAGEOAUDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOAU.Utils.ToolBox.DIAGEOAUDIAGEOToolBox.download_template', return_value=diageoau_template)
    @seeder.seed(["diageoau_seed"], ProjectsSanityData())
    def test_diageoau_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['9B69BB21-3D7C-4899-9465-41819978D810']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOAUCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
