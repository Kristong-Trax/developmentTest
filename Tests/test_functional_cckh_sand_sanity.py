
import os
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Tests.Data.TestData.test_data_cckh_sand_sanity import ProjectsSanityData
from Projects.CCKH_SAND.Calculations import CCKH_SANDCalculations

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
    
    def _assert_old_tables_kpi_results_filled(self, distinct_kpis_num=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor()
        cursor.execute('''
           SELECT * FROM report.kpi_results
           ''')
        kpi_results = cursor.fetchall()
        if distinct_kpis_num:
            df = pd.DataFrame(kpi_results)
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_new_tables_kpi_results_filled(self, distinct_kpis_num=None, list_of_kpi_names=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor()
        cursor.execute('''
           SELECT kl2.pk, kl2.client_name, kl2r.kpi_level_2_fk, kl2r.result 
           FROM report.kpi_level_2_results kl2r left join static.kpi_level_2 kl2 
           on kpi_level_2_fk = kl2.pk
           ''')
        kpi_results = cursor.fetchall()
        df = pd.DataFrame(kpi_results)
        if distinct_kpis_num:
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        if list_of_kpi_names:
            exisitng_results = df['client_name'].unique()
            result = all(elem in exisitng_results for elem in list_of_kpi_names)
            self.assertTrue(result)
        connector.disconnect_rds()

    def _assert_scene_tables_kpi_results_filled(self, distinct_kpis_num=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor()
        cursor.execute('''
           SELECT * FROM report.scene_kpi_results
           ''')
        kpi_results = cursor.fetchall()
        if distinct_kpis_num:
            df = pd.DataFrame(kpi_results)
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    @seeder.seed(["cckh_sand_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_cckh_sand_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'ff0d57ad-a7ed-46ca-9488-1d28f4682500': []}
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            CCKH_SANDCalculations(data_provider, output).run_project_calculations()
            # self._assert_old_tables_kpi_results_filled(distinct_kpis_num=None)
            # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
            # for scene in sessions[session]:
            #     data_provider.load_scene_data(str(session), scene_id=scene)
            #     SceneCalculations(data_provider).calculate_kpis()
            #     self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
