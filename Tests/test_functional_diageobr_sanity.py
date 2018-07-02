
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.TestData.test_data_diageobr_sanity import ProjectsSanityData
from mock import patch

from Projects.DIAGEOBR.Calculations import DIAGEOBRCalculations
from Tests.Data.Templates.diageobr_template import diageobr_template

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
    
    @patch('Projects.DIAGEOBR.Utils.ToolBox.DIAGEOBRDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-02-20')
    @patch('Projects.DIAGEOBR.Utils.ToolBox.DIAGEOBRDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOBR.Utils.ToolBox.DIAGEOBRDIAGEOToolBox.download_template',
           return_value=diageobr_template)
    @seeder.seed(["diageobr_seed"], ProjectsSanityData())
    def test_diageobr_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['0b3fa9ee-3c9a-46c2-8107-81eb88efa22c']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOBRCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
