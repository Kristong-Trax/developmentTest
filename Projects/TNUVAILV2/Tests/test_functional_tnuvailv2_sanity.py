from Projects.TNUVAILV2.Tests.Data.TestData.test_data_tnuvailv2 import TnuvailV2SanityData
from Trax.Data.Testing.SeedNew import Seeder
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Projects.TNUVAILV2.Calculations import Calculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output

import MySQLdb
import os


class TestTnuvailV2Sanity(TestFunctionalCase):

    def set_up(self):
        super(TestTnuvailV2Sanity, self).set_up()
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

    @seeder.seed(["mongodb_products_and_brands_seed", "tnuvailv2_sand_seed"], TnuvailV2SanityData())
    def test_tnuvailv2_sanity(self):
        project_name = TnuvailV2SanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['236c1577-0ecb-4bf9-88b9-c9e87ab17c58']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
