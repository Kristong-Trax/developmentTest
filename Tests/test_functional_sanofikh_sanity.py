
import os
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.TestData.test_data_sanofikh_sanity import ProjectsSanityData
from Projects.SANOFIKH.Calculations import SANOFIKHCalculations
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
    
    @seeder.seed(["sanofikh_seed"], ProjectsSanityData())
    def test_sanofikh_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['42eb6073-198e-4dc9-af67-91b4f4f800ae']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            SANOFIKHCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
