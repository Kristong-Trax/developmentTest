
import os
import MySQLdb
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
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
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
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
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

    def _assert_test_results_matches_reality(self):
        real_res_dict = pd.DataFrame({'numerator_id': {0: 1558, 1: 118, 2: 118, 3: 4, 4: 20, 5: 1489, 6: 118, 7: 118, 8: 1735, 9: 20, 10: 1489, 11: 1009, 12: 118, 13: 118, 14: 4, 15: 20, 16: 45, 17: 0, 18: 4, 19: 4, 20: 0, 21: 0, 22: 0, 23: 0, 24: 102, 25: 0, 26: 0}, 'kpi_level_2_fk': {0: 1001, 1: 1007, 2: 1022, 3: 1016, 4: 1024, 5: 1012, 6: 1011, 7: 1019, 8: 1013, 9: 1021, 10: 1020, 11: 1009, 12: 1010, 13: 1027, 14: 1026, 15: 1028, 16: 1029, 17: 9000, 18: 3006, 19: 9001, 20: 1030, 21: 9002, 22: 1032, 23: 9003, 24: 9004, 25: 1031, 26: 9005}, 'context_id': {0: None, 1: None, 2: None, 3: None, 4: None, 5: None, 6: None, 7: None, 8: None, 9: None, 10: None, 11: None, 12: None, 13: None, 14: None, 15: None, 16: None, 17: 6.0, 18: None, 19: 6.0, 20: None, 21: 6.0, 22: None, 23: 6.0, 24: 6.0, 25: None, 26: 6.0}, 'client_name': {0: u'Distribution', 1: u'Distribution - SKU', 2: u'Distribution - SKU BY CATEGORY', 3: u'Distribution BY CATEGORY', 4: u'Distribution BY MANUFACTURER', 5: u'LMPA', 6: u'LMPA - SKU', 7: u'LMPA - SKU BY CATEGORY', 8: u'LMPA ASSORTMENT GROUP', 9: u'LMPA ASSORTMENT GROUP BY MANUFACTURER', 10: u'LMPA BY CATEGORY', 11: u'OOS',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  12: u'OOS - SKU', 13: u'OOS - SKU BY CATEGORY', 14: u'OOS BY CATEGORY', 15: u'OOS BY MANUFACTURER', 16: u'SOS BRAND OUT OF MANUFACTURER', 17: u'SOS BRAND OUT OF SUB CATEGORY BY SCENE TYPE', 18: u'SOS CATEGORY OUT OF STORE', 19: u'SOS CATEGORY OUT OF STORE BY SCENE TYPE', 20: u'SOS MANUFACTURER OUT OF SUB CATEGORY', 21: u'SOS MANUFACTURER OUT OF SUB CATEGORY BY SCENE TYPE', 22: u'SOS OUT OF STORE', 23: u'SOS OUT OF STORE BY SCENE TYPE', 24: u'SOS PRODUCT OUT OF BRAND BY SCENE TYPE', 25: u'SOS SUB CATEGORY OUT OF CATEGORY', 26: u'SOS SUB CATEGORY OUT OF CATEGORY BY SCENE TYPE'}, 'result': {0: 40.0, 1: 0.0, 2: 1.0, 3: 80.0, 4: 40.0, 5: 0.0, 6: 0.0, 7: 1.0, 8: 40.0, 9: 40.0, 10: 0.0, 11: 60.0, 12: 1.0, 13: 1.0, 14: 20.0, 15: 60.0, 16: 1.0, 17: 1.0, 18: 1.0, 19: 1.0, 20: 1.0, 21: 1.0, 22: 0.518868, 23: 0.518868, 24: 1.0, 25: 0.518868, 26: 0.518868}, 'denominator_id': {0: None, 1: 1558.0, 2: 5.0, 3: 20.0, 4: 3506.0, 5: 1735.0, 6: 1489.0, 7: 1662.0, 8: 0.0, 9: 3506.0, 10: 20.0, 11: 0.0, 12: 1009.0, 13: 5.0, 14: 20.0, 15: 3506.0, 16: 15.0, 17: 0.0, 18: 3506.0, 19: 3506.0, 20: 0.0, 21: 0.0, 22: 3506.0, 23: 3506.0, 24: 45.0, 25: 4.0, 26: 4.0}})

        real_results = pd.DataFrame(real_res_dict)

        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
                        SELECT 
                            distinct kpi.client_name,res.kpi_level_2_fk, numerator_id, denominator_id, context_id, result
                        FROM
                            report.kpi_level_2_results res
                                LEFT JOIN
                            static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
                                LEFT JOIN
                            probedata.session ses ON ses.pk = res.session_fk
           ''')
        kpi_results = cursor.fetchall()
        kpi_results = pd.DataFrame(kpi_results)
        merged_results = pd.merge(real_results, kpi_results, on=['kpi_level_2_fk', 'numerator_id', 'denominator_id',
                                                                 'context_id'], how="left")
        wrong_results = merged_results[merged_results['result_x'] != merged_results['result_y']]
        if not wrong_results.empty:
            print "The following KPIs had wrong results:"
            for i, res in wrong_results.iterrows():
                print "kpi_level_2_fk: {0}, client_name: {1}, numerator_id: {2}, denominator_id: {3}, "                       "context_id: {4}".format(str(res['kpi_level_2_fk']), str(res['client_name_x']),
                                                                                                                                                       str(res['numerator_id']), str(
                                                                                                                                                           res['denominator_id']),
                                                                                                                                                       str(res['context_id']))
        self.assertTrue(wrong_results.empty)

    def _assert_scene_tables_kpi_results_filled(self, distinct_kpis_num=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
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
