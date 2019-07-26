
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_pngcn_prod_sanity import ProjectsSanityData
from Projects.PNGCN_PROD.Calculations import PngCNEmptyCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Projects.PNGCN_PROD.SceneKpis.SceneCalculations import SceneCalculations
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'ilays'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()
        self.mock_object(object_name='commit_results_data', path='KPIUtils_v2.DB.CommonV2.Common')
        self.mock_object(object_name='get_display_group',
                         path='Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis')

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_old_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_new_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_scene_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
         SELECT * FROM report.scene_kpi_results
         ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    @seeder.seed(["pngcn_prod_seed"], ProjectsSanityData())
    def test_pngcn_prod_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['4E75E4F1-F5F0-4B77-8337-513F61913864']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            PngCNEmptyCalculations(data_provider, output).run_project_calculations()
            self._assert_new_tables_kpi_results_filled()
            self._assert_old_tables_kpi_results_filled()
            data_provider.load_scene_data(session, scene_id=19626328)
            SceneCalculations(data_provider).calculate_kpis()
            self._assert_scene_kpi_results_filled()
