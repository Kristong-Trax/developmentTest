
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_sanofijp_sanity import ProjectsSanityData
from Projects.SANOFIJP.Calculations import SANOFIJPCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase


__author__ = 'nidhin'


class TestKEngineOutOfTheBox(TestFunctionalCase):

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
    
    @seeder.seed(["sanofijp_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_sanofijp_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['4CD0B144-F8EA-4C2C-AA51-A8FB63FF4C23']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            SANOFIJPCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
