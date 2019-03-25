
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.TestData.test_data_gfkde_sanity import ProjectsSanityData
from Projects.GFKDE.Calculations import GFKDECalculations
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'elis'


class TestKEngineOutOfTheBox(TestMockingFunctionalCase):

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
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        select * from report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    @seeder.seed(["gfkde_seed"], ProjectsSanityData())
    def test_gfkde_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['9117364e-2b00-4c1d-b4ab-f1ff1a2a5bde']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            GFKDECalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
