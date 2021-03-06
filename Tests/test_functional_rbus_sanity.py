
import os

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_rbus_sanity import ProjectsSanityData
from Projects.RBUS.Calculations import RBUSRBUSCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'


class TestKEngineOutOfTheBox(TestFunctionalCase):
    
    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()
    
    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor()
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    @seeder.seed(["rbus_seed"], ProjectsSanityData())
    def test_rbus_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['a08a89f7-6ff8-41b1-9685-b78b32374f33']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            RBUSRBUSCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
