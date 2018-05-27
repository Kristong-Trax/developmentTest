# import os
#
# from Trax.Data.Testing.SeedNew import Seeder
# from pandas.util.testing import assert_frame_equal
# from Trax.Utils.Testing.Case import TestCase
# from mock import patch
# import pandas as pd
# from Projects.RIPETCAREUK_PROD.Tests.data_test_unit_marsuk import DataTestUnitMarsUk
# from Projects.RIPETCAREUK_PROD.Tools.CreateHierarchy import CreateMarsUkKpiHierarchy
# from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import KPIConsts
#
#
# class TestHirarchy(TestCase):
#
#     seeder = Seeder()
#     mock_path = 'Projects.RIPETCAREUK_PROD.Tools.CreateHierarchy'
#
#     def setUp(self):
#         super(TestCase, self).setUp()
#         self.patchers = []
#         self.mock_template_path()
#
#     def _mock_template(self, template_data):
#         template_patcher = patch('{}.ParseMarsUkTemplates'.format(self.mock_path))
#         template_mock = template_patcher.start()
#         template_mock.return_value.parse_templates.return_value = template_data
#         self.patchers.append(template_patcher)
#
#     def tearDown(self):
#         super(TestCase, self).tearDown()
#         for patcher in self.patchers:
#             patcher.stop()
#
#     def mock_template_path(self):
#         template_patcher = patch('Projects.RIPETCAREUK_PROD.Utils.ParseTemplates.TEMPLATE_NAME',
#                                  os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Test_Template.xlsx'))
#         template_patcher.start()
#         self.patchers.append(template_patcher)
#
#     def test_hierarchy(self):
#         """
#         Testing the hierarchy is built according to template. atomics which are not referenced from top levels or
#         not added to hierarchy
#         :return:
#         """
#
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.assortment_kpi_data,
#             KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.num_of_facings_atomic_kpi_data,
#             KPIConsts.FACINGS_SHEET: DataTestUnitMarsUk.facings_atomic_kpi_data_with_test_store_type
#         }
#         self._mock_template(template_data)
#         kpi_level_hierarchy_creator = CreateMarsUkKpiHierarchy()
#         kpi_level_hierarchy_creator.create_hierarchy()
#         kpi_level_1 = kpi_level_hierarchy_creator.kpi_level_1_hierarchy
#         assert_frame_equal(kpi_level_1, pd.DataFrame({
#             'set_name': ['PERFECT STORE', 'ASSORTMENT SCORE']
#         }))
#
#         kpi_level_2 = kpi_level_hierarchy_creator.kpi_level_2_hierarchy
#         assert_frame_equal(kpi_level_2, pd.DataFrame(
#             columns=['set_name', 'kpi_name'],
#             data={
#                 'set_name': ['PERFECT STORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE'],
#                 'kpi_name': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch']
#         }))
#
#         kpi_level_3 = kpi_level_hierarchy_creator.kpi_level_3_hierarchy
#         assert_frame_equal(kpi_level_3, pd.DataFrame(
#             columns=['set_name', 'kpi_name', 'atomic_name'],
#             data={
#                 'atomic_name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
#                                 u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
#                                 u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
#                                 u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
#                 'kpi_name': [u'Sheba', u'Sheba', u'Whi Pouch', u'Whi Pouch'],
#                 'set_name': [u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE']}))
#
#     def test_hierarchy_duplicated_are_dropped(self):
#         """
#         Testing the hierarchy is built according to template. atomics which are not referenced from top levels or
#         not added to hierarchy
#         :return:
#         """
#
#         template_data = {
#             KPIConsts.SHEET_NAME: DataTestUnitMarsUk.hierarchy_duplicated_kpi_data,
#             KPIConsts.ASSORTMENT_SHEET: DataTestUnitMarsUk.hierarchy_duplicated_atomic_kpi_data,
#             KPIConsts.FACINGS_SHEET: DataTestUnitMarsUk.facings_atomic_kpi_data_with_test_store_type
#         }
#         self._mock_template(template_data)
#         kpi_level_hierarchy_creator = CreateMarsUkKpiHierarchy()
#         kpi_level_hierarchy_creator.create_hierarchy()
#         kpi_level_1 = kpi_level_hierarchy_creator.kpi_level_1_hierarchy
#         assert_frame_equal(kpi_level_1, pd.DataFrame({
#             'set_name': ['PERFECT STORE', 'ASSORTMENT SCORE']
#         }))
#
#         kpi_level_2 = kpi_level_hierarchy_creator.kpi_level_2_hierarchy
#         assert_frame_equal(kpi_level_2, pd.DataFrame(
#             columns=['set_name', 'kpi_name'],
#             data={
#                 'set_name': ['PERFECT STORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE'],
#                 'kpi_name': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch']
#         }))
#
#         kpi_level_3 = kpi_level_hierarchy_creator.kpi_level_3_hierarchy
#         assert_frame_equal(kpi_level_3, pd.DataFrame(
#             columns=['set_name', 'kpi_name', 'atomic_name'],
#             data={
#                 'atomic_name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
#                                 u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
#                                 u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
#                                 u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
#                 'kpi_name': [u'Sheba', u'Sheba', u'Whi Pouch', u'Whi Pouch'],
#                 'set_name': [u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE']}))
#
#
#
