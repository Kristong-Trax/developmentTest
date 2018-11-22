# import os
# import pandas as pd
# from MySQLdb.cursors import DictCursor
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestCase, skip
# from Trax.Utils.Testing.ConfigTest import TempTestConfig
# from mock import patch
#
# from Projects.RIPETCAREUK_PROD.Calculation import MarsUkCalculations
# from Projects.RIPETCAREUK_PROD.Exceptions import AtomicKpiNotInStaticException, KpiNotInStaticException, \
#     KpiSetNotInStaticException
# from Projects.RIPETCAREUK_PROD.Tests.TestWriter import TestKpiResultsWriter
# from Projects.RIPETCAREUK_PROD.Tests.data_test_functional_marsuk import DataTestMarsuk
# from Projects.RIPETCAREUK_PROD.Tests.data_test_unit_marsuk import DataTestUnitMarsUk
# from Projects.RIPETCAREUK_PROD.Tools.UpdataStaticData import UpdateStaticData
# from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import KPIConsts
#
#
# class TestMarsuk(TestCase):
#
#     seeder = Seeder()
#     config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
#     mock_path = 'Projects.RIPETCAREUK_PROD.Utils.KPIToolBox'
#
#     def setUp(self):
#         super(TestCase, self).setUp()
#         self.patchers = []
#         self.mock_template_path()
#
#     def tearDown(self):
#         super(TestCase, self).tearDown()
#         for patcher in self.patchers:
#             patcher.stop()
#
#     def mock_template_path(self):
#         template_patcher = patch('Projects.RIPETCAREUK_PROD.Utils.ParseTemplates.TEMPLATE_NAME',
#                                  os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Test_Template.xlsx'))
#         template_patcher.start()
#         self.patchers.append(template_patcher)
#
#     def _mock_template(self, template_data):
#         template_patcher = patch('{}.ParseMarsUkTemplates'.format(self.mock_path))
#         template_mock = template_patcher.start()
#         template_mock.return_value.parse_templates.return_value = template_data
#         self.patchers.append(template_patcher)
#
#     def mock_writer_kpi(self):
#
#         patcher = patch('{}.MarsUkPerfectScore._get_writer'.format(self.mock_path))
#         self._writer_mock = patcher.start()
#         writer = TestKpiResultsWriter()
#         self._writer_mock.return_value = writer
#         self.patchers.append(patcher)
#
#     def delete_atomic_kpi_by_atomic_and_set(self, atomic_name, set_name):
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = """
#         delete static.atomic_kpi from static.atomic_kpi
#         join static.kpi on static.kpi.pk = static.atomic_kpi.kpi_fk
#         join static.kpi_set on static.kpi_set.pk = static.kpi.kpi_set_fk
#         where static.atomic_kpi.name = '{}' and static.kpi_set.name = '{}';""".format(atomic_name, set_name)
#         cur = rds_conn.db.cursor(DictCursor)
#         cur.execute(query)
#         rds_conn.db.commit()
#
#     def delete_kpi_by_name_and_set(self, kpi_name, set_name):
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = """
#         delete static.kpi from static.kpi
#         join static.kpi_set on static.kpi_set.pk = static.kpi.kpi_set_fk
#         where static.kpi.display_text = '{}' and static.kpi_set.name = '{}';""".format(kpi_name, set_name)
#         cur = rds_conn.db.cursor(DictCursor)
#         cur.execute(query)
#         rds_conn.db.commit()
#
#     def delete_kpi_set_by_name(self, set_name):
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = """
#         delete from static.kpi_set
#         where static.kpi_set.name = '{}';""".format(set_name)
#         cur = rds_conn.db.cursor(DictCursor)
#         cur.execute(query)
#         rds_conn.db.commit()
#
#     @staticmethod
#     def _get_kpk_results():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from report.kpk_results;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @staticmethod
#     def _get_kpi_results():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from report.kpi_results;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @staticmethod
#     def _get_kps_results():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from report.kps_results;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @seeder.seed(["sql_seed"], DataTestMarsuk())
#     def test_marsuk_sanity(self):
#         with TempTestConfig(conf_file=self.config_file):
#             project_name = 'test_project_1'
#             UpdateStaticData(project_name).update_static_data()
#             data_provider = KEngineDataProvider(project_name)
#             session = '2459490b-8333-41fc-be40-3ac046dfb885'
#             data_provider.load_session_data(session)
#             output = Output()
#             MarsUkCalculations(data_provider, output).run_project_calculations()
#             kpk_results = self._get_kpk_results()
#             kpi_results = self._get_kpi_results()
#             kps_results = self._get_kps_results()
#
#             self.assertEquals(len(kpk_results.index), 18)
#             self.assertEquals(len(kps_results.index), 7)
#             self.assertEquals(len(kpi_results.index), 18)
#
#     # Shelves kpi's are tested in functional tests instead of unit tests due to the complexity of the input data.
#
#     @seeder.seed(["shelves_not_correct"], DataTestMarsuk())
#     def test_shelves_only_on_wrong_shelves(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[(results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,') &
#                                          (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0)
#             self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)
#
#     def run_calculations(self):
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         data_provider = KEngineDataProvider(project_name)
#         session = '2459490b-8333-41fc-be40-3ac046dfb885'
#         data_provider.load_session_data(session)
#         output = Output()
#         MarsUkCalculations(data_provider, output).run_project_calculations()
#         results = self._writer_mock.return_value._kpi_results
#         return results
#
#     @seeder.seed(["shelves_not_correct"], DataTestMarsuk())
#     def test_shelves_only_on_wrong_shelves_on_different_scene_type(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data_with_different_scene_type_filter,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[
#                 (results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,') &
#                 (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 1)
#
#     @seeder.seed(["shelves_not_correct"], DataTestMarsuk())
#     def test_kpi_results_saves_correct_fields(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[
#                 (results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,') &
#                 (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0)
#             self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)
#
#     @seeder.seed(["right_shelves_bottom"], DataTestMarsuk())
#     def test_shelves_only_on_right_shelves(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[
#                 (results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,') &
#                 (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 1)
#             self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)
#
#     @seeder.seed(["mixed_shelves_bottom"], DataTestMarsuk())
#     def test_shelves_only_on_mixed_shelves(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[
#                 (results['atomic_kpi_name'] == 'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,') &
#                 (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 0)
#             self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 0)
#
#     @seeder.seed(["product_not_distributed"], DataTestMarsuk())
#     def test_product_not_distributed_score_100(self):
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.shelves_kpi_data,
#             KPIConsts.SHELVES_POS_SHEET: DataTestUnitMarsUk.shelves_atomic_kpi_data
#         }
#         self._mock_template(template_data)
#         self.mock_writer_kpi()
#         with TempTestConfig(conf_file=self.config_file):
#             results = self.run_calculations()
#             sheba_fresh_choice = results[
#                 (results['atomic_kpi_name'] == 'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,') &
#                 (results['set_name'] == 'Position on Shelf - Position score')]
#             self.assertEquals(sheba_fresh_choice.iloc[0]['result'], 1)
#             self.assertEquals(sheba_fresh_choice.iloc[0]['score'], 100)
#
#
#
#
