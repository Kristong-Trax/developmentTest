from Projects.NESTLEIL.Tests.TestsData.test_data_nestleil import NestleILSanityData
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Projects.NESTLEIL.Calculations import Calculations
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Tests.TestUtils import remove_cache_and_storage
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
import os


class TestNestleILSanity(TestFunctionalCase):

    def set_up(self):
        super(TestNestleILSanity, self).set_up()
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

    @seeder.seed(["mongodb_products_and_brands_seed", "nestleil_seed"], NestleILSanityData())
    def test_nestleil_sanity(self):
        project_name = NestleILSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['ad083ee9-71b6-4b62-8168-8e4015635799']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
