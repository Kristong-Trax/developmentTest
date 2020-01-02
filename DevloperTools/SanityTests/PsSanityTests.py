import os
import MySQLdb
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage
from Trax.Utils.Logging.Logger import Log

__author__ = 'ilays'


class PsSanityTestsFuncs(TestFunctionalCase):

    seeder = Seeder()

    def set_up(self):
        super(PsSanityTestsFuncs, self).set_up()
        remove_cache_and_storage()

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    def _assert_old_tables_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.kpi_results
           ''')
        kpi_results = cursor.fetchall()
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
            existing_results = df['client_name'].unique()
            result = all(elem in existing_results for elem in list_of_kpi_names)
            self.assertTrue(result)
        connector.disconnect_rds()

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

    def _assert_test_results_matches_reality(self, kpi_results):
        real_res_dict = pd.DataFrame(kpi_results)
        real_results = pd.DataFrame(real_res_dict)
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""
                        SELECT 
                        distinct kpi.client_name, res.session_fk, res.kpi_level_2_fk, numerator_id, 
                        denominator_id, context_id, result
                        FROM
                            report.kpi_level_2_results res
                                LEFT JOIN
                            static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
                                LEFT JOIN
                            probedata.session ses ON ses.pk = res.session_fk
           """)
        kpi_results = cursor.fetchall()
        self.assertFalse(len(kpi_results) == 0)
        kpi_results = pd.DataFrame(kpi_results)
        merged_results = pd.merge(real_results, kpi_results, on=['session_fk', 'kpi_level_2_fk', 'numerator_id',
                                                                 'denominator_id', 'context_id'], how="left")
        wrong_results = merged_results[merged_results['result_x'] != merged_results['result_y']]
        if not wrong_results.empty:
            print "The following KPIs had wrong results:"
            for i, res in wrong_results.iterrows():
                print ("session_fk: {0}, kpi_level_2_fk: {1}, client_name: {2}, numerator_id: {3}, "
                       "denominator_id: {4}, context_id: {5}, seed_result: {6}, "
                       "db_actual_result: {7}".format(str(res['session_fk']), str(res['kpi_level_2_fk']),
                                                      str(res['client_name_x']), str(res['numerator_id']),
                                                      str(res['denominator_id']), str(res['context_id']),
                                                      str(res['result_y']), str(res['result_x'])))
        self.assertTrue(wrong_results.empty)
