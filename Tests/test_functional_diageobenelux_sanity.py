
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageobenelux_sand_template import diageobenelux_sand_template
from Tests.Data.Templates.diageobenelux_template import diageobenelux_template
from Tests.Data.TestData.test_data_diageobenelux_sanity import ProjectsSanityData
from Projects.DIAGEOBENELUX.Calculations import DIAGEOBENELUXCalculations


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

    @patch('Projects.DIAGEOBENELUX.Utils.ToolBox.DIAGEOBENELUXDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-02-20')
    @patch('Projects.DIAGEOBENELUX.Utils.ToolBox.DIAGEOBENELUXDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOBENELUX.Utils.ToolBox.DIAGEOBENELUXDIAGEOToolBox.download_template',
           return_value=diageobenelux_template)
    @seeder.seed(["diageobenelux_seed"], ProjectsSanityData())
    def test_diageobenelux_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['A2B4210B-85CF-45CC-A7F7-839BC7734BAA']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOBENELUXCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
