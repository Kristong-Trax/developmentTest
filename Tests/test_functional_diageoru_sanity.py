
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase, skip
from mock import patch

from Tests.Data.TestData.test_data_diageoru_sanity import ProjectsSanityData
from Projects.DIAGEORU.Calculations import DIAGEORUDIAGEORUCalculations
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
        self.assertNotEquals(0, 0)
        connector.disconnect_rds()

    @skip('Test failed in garage')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.save_latest_templates')
    @seeder.seed(["diageoru_seed"], ProjectsSanityData())
    def test_diageoru_sanity(self, x, y):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['503d5abf-7359-45bf-859c-03c5211fd2db']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEORUDIAGEORUCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
