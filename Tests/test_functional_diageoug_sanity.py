import os
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Tests.Data.TestData.test_data_diageoug_sanity import ProjectsSanityData
from Projects.DIAGEOUG.Calculations import DIAGEOUGCalculations

from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'ilays'


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
            df = pd.DataFrame(list(kpi_results), columns=[col[0] for col in cursor.description])
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
        df = pd.DataFrame(list(kpi_results), columns=[col[0] for col in cursor.description])
        if distinct_kpis_num:
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        if list_of_kpi_names:
            exisitng_results = df['client_name'].unique()
            result = all(elem in exisitng_results for elem in list_of_kpi_names)
            self.assertTrue(result)
        connector.disconnect_rds()

    # def _assert_test_results_matches_reality(self):
    #
    #     connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
    #     cursor = connector.db.cursor()
    #     cursor.execute('''
    #                     SELECT
    #                         distinct kpi.client_name,res.kpi_level_2_fk, numerator_id, denominator_id, context_id, result
    #                     FROM
    #                         report.kpi_level_2_results res
    #                             LEFT JOIN
    #                         static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
    #                             LEFT JOIN
    #                         probedata.session ses ON ses.pk = res.session_fk
    #        ''')
    #     kpi_results = cursor.fetchall()
    #     kpi_results = pd.DataFrame(kpi_results)
    #     merged_results = pd.merge(real_results, kpi_results, on=['kpi_level_2_fk', 'numerator_id', 'denominator_id',
    #                                                              'context_id'], how="left")
    #     wrong_results = merged_results[merged_results['result_x'] != merged_results['result_y']]
    #     if not wrong_results.empty:
    #         print "The following KPIs had wrong results:"
    #         for i, res in wrong_results.iterrows():
    #             print "kpi_level_2_fk: {0}, client_name: {1}, numerator_id: {2}, denominator_id: {3}, "                       "context_id: {4}".format(
    #                 str(res['kpi_level_2_fk']), str(res['client_name_x']),
    #                 str(res['numerator_id']), str(
    #                     res['denominator_id']),
    #                 str(res['context_id']))
    #     self.assertTrue(wrong_results.empty)

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

    @seeder.seed(["diageoug_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_diageoug_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'f9d6b8a5-7964-4ef5-afe4-8580df97f57c': []}
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            DIAGEOUGCalculations(data_provider, output).run_project_calculations()
            # self._assert_old_tables_kpi_results_filled(distinct_kpis_num=None)
            # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
            # self._assert_test_results_matches_reality()
            # for scene in sessions[session]:
            #     data_provider.load_scene_data(str(session), scene_id=scene)
            #     SceneCalculations(data_provider).calculate_kpis()
            #     self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
