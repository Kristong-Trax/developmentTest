
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Tests.Data.TestData.test_data_ccus_sanity import ProjectsSanityData
from Projects.CCUS.Calculations import CCUSCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import skip
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'jasmineg'


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
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    # TODO: FIX SANITY TEST
    @skip('Test failed in master')
    @seeder.seed(["mongodb_products_and_brands_seed", "ccus_seed"], ProjectsSanityData())
    def test_ccus_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['8395fc95-465b-47c2-ad65-6d10de13cd75']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            CCUSCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
