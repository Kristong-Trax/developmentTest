# import os
# import pandas as pd
# from pandas.util.testing import assert_frame_equal
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestCase
# from mock import patch
# from Projects.RIPETCAREUK_PROD.Tools.Tests.data_test_functional_marsuk import DataTestStaticUpdate
# from Projects.RIPETCAREUK_PROD.Tools.UpdataStaticData import UpdateStaticData
#
# __author__ = 'Dudi S'
#
#
# class TestMarsuk(TestCase):
#
#     seeder = Seeder()
#     config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
#     mock_path = 'Projects.RIPETCAREUK_PROD.Tools.UpdataStaticData'
#
#     def setUp(self):
#         super(TestCase, self).setUp()
#         self.patchers = []
#         self.mock_template_path()
#         self.mock_hierarchy()
#
#     def tearDown(self):
#         super(TestCase, self).tearDown()
#         for patcher in self.patchers:
#             patcher.stop()
#
#     def mock_hierarchy(self):
#         patcher = patch('{}.CreateMarsUkKpiHierarchy'.format(self.mock_path))
#         mock = patcher.start()
#         self.hierarchy_mock = mock.return_value
#         self.patchers.append(patcher)
#
#     def mock_hierarchy_data(self, data):
#         self.hierarchy_mock.kpi_level_1_hierarchy = data['level_1']
#         self.hierarchy_mock.kpi_level_2_hierarchy = data['level_2']
#         self.hierarchy_mock.kpi_level_3_hierarchy = data['level_3']
#
#     def mock_template_path(self):
#         template_patcher = patch('Projects.RIPETCAREUK_PROD.Utils.ParseTemplates.TEMPLATE_NAME',
#                                  os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Test_Template.xlsx'))
#         template_patcher.start()
#         self.patchers.append(template_patcher)
#
#     @staticmethod
#     def _get_static_kpi_set():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from static.kpi_set;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @staticmethod
#     def _get_static_atomic_kpi():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from static.atomic_kpi;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @staticmethod
#     def _get_static_kpi():
#         rds_conn = PSProjectConnector('test_project_1', DbUsers.ReadOnly)
#         query = 'select * from static.kpi;'
#         return pd.read_sql_query(query, rds_conn.db)
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_kpi_set_updated(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         kpi_set = self._get_static_kpi_set()
#         columns = ['pk', 'name']
#         assert_frame_equal(kpi_set[columns], pd.DataFrame(
#             columns=columns,
#             data={
#                 'name': [u'PERFECT STORE', u'ASSORTMENT SCORE'],
#                 'pk': [1, 2]
#             }
#         ))
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_kpi_updated(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         kpi = self._get_static_kpi()
#         columns = ['pk', 'kpi_set_fk', 'display_text']
#         assert_frame_equal(kpi[columns], pd.DataFrame(
#             columns=columns,
#             data={
#                 'display_text': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch'],
#                 'kpi_set_fk': [1, 2, 2],
#                 'pk': [1, 2, 3],
#             }
#         ))
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_atomic_kpi_updated(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         columns = ['pk', 'kpi_fk', 'name']
#         assert_frame_equal(atomic_kpi[columns], pd.DataFrame(
#             columns=columns,
#             data={
#                 'kpi_fk': [2, 2, 3, 3],
#                 'name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
#                          u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
#                          u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
#                          u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
#                 'pk': [1, 2, 3, 4]
#             }
#         ))
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_if_already_exists_does_not_create_new(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         kpi = self._get_static_kpi()
#         kpi_set = self._get_static_kpi_set()
#         self.assertEquals(len(atomic_kpi.index), 4)
#         self.assertEquals(len(kpi.index), 3)
#         self.assertEquals(len(kpi_set.index), 2)
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_if_not_exists_in_the_same_name_create_new(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_two)
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         kpi = self._get_static_kpi()
#         kpi_set = self._get_static_kpi_set()
#         self.assertEquals(len(atomic_kpi.index), 5)
#         self.assertEquals(len(kpi.index), 4)
#         self.assertEquals(len(kpi_set.index), 3)
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_if_not_exists_in_the_same_name_create_new(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_two)
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         kpi = self._get_static_kpi()
#         kpi_set = self._get_static_kpi_set()
#         self.assertEquals(len(atomic_kpi.index), 5)
#         self.assertEquals(len(kpi.index), 4)
#         self.assertEquals(len(kpi_set.index), 3)
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_if_not_exists_in_the_same_name_create_new(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_two)
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         kpi = self._get_static_kpi()
#         kpi_set = self._get_static_kpi_set()
#         self.assertEquals(len(atomic_kpi.index), 5)
#         self.assertEquals(len(kpi.index), 4)
#         self.assertEquals(len(kpi_set.index), 3)
#
#     @seeder.seed(["sql_seed"], DataTestStaticUpdate())
#     def test_if_not_exists_with_the_same_parent_create_new(self):
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_one)
#         project_name = 'test_project_1'
#         UpdateStaticData(project_name).update_static_data()
#         self.mock_hierarchy_data(DataTestStaticUpdate.hierarchy_three)
#         UpdateStaticData(project_name).update_static_data()
#         atomic_kpi = self._get_static_atomic_kpi()
#         kpi = self._get_static_kpi()
#         kpi_set = self._get_static_kpi_set()
#         self.assertEquals(len(atomic_kpi.index), 6)
#         self.assertEquals(len(kpi.index), 5)
#         self.assertEquals(len(kpi_set.index), 3)
#
#
