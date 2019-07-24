
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_singhath_sanity import ProjectsSanityData
from Projects.SINGHATH.Calculations import Calculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'nidhin'


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
    
    @seeder.seed(["singhath_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_singhath_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['59954969-63a0-4e89-9283-12386f57049d']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
