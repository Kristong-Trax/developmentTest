# #
# # from Trax.Utils.Conf.Configuration import Config
# # from Trax.Utils.Testing.Case import TestCase
# # from mock import MagicMock, mock
# # import pandas as pd
# # from Projects.CCBZA_SAND.Utils.KPIToolBox import CCBZAToolBox
# #
# #
# # __author__ = 'natalyak'
# #
# #
# # class TestCCBZA_SAND(TestCase):
# #
# #     @mock.patch('Projects.CCBZA_SAND.Utils.KPIToolBox.ProjectConnector')
# #     def setUp(self, x):
# #         Config.init('')
# #         self.data_provider_mock = MagicMock()
# #         self.data_provider_mock.project_name = 'ccbza-sand'
# #         self.data_provider_mock.rds_conn = MagicMock()
# #         self.output = MagicMock()
# #         self.tool_box = CCBZAToolBox(self.data_provider_mock, MagicMock())
# #
#
# from Trax.Utils.Testing.Case import TestCase, MockingTestCase
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Data.Projects.Connector import ProjectConnector
# from mock import MagicMock
# from Projects.CCBZA_SAND.Utils.KPIToolBox import CCBZA_SANDToolBox
# from Projects.CCBZA_SAND.Tests.data_test_unit_ccbza_sand import DataTestUnitCCBZA_SAND, DataScores
# from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template
# from Trax.Algo.Calculations.Core.DataProvider import Output
# from mock import patch
# import os
# import pandas as pd
# from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
#     SET_NAME, KPI_NAME
#
#
# class TestCCBZA_SAND(MockingTestCase):
#     seeder = Seeder()
#
#     @property
#     def import_path(self):
#         return 'Projects.CCBZA_SAND.Utils.KPIToolBox'
#
#     # mock_path = 'Projects.CCBZA_SAND.Utils.KPIToolBox'
#
#     def setUp(self):
#         super(TestCCBZA_SAND, self).setUp()
#         self.mock_data_provider()
#         self.data_provider_mock.project_name = 'Test_Project_1'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.project_connector_mock = self.mock_project_connector()
#         self.static_kpi_mock = self.mock_static_kpi()
#         # self._mock_match_display_in_scene()
#         # self._mock_match_stores_by_retailer()
#         # self._mock_match_template_fk_by_category_fk()
#         self.store_data_mock = self.mock_store_data()
#         # self._main_calculation_mock = self.mock_object('CBCILCBCIL_ToolBox.main_calculation')
#         # self._CBCIL_GeneralToolBox_mock = self._mock_general_toolbox()
#         # self._scores = DataScores()
#         self.template_mock = self.mock_template_data()
#         self.output = MagicMock()
#
#     def mock_data_provider(self):
#         self.data_provider_mock = MagicMock()
#         # return self._data_provider
#         self.data_provider_data_mock = {}
#         def get_item(key):
#             return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()
#         self.data_provider_mock.__getitem__.side_effect = get_item
#
#     def mock_project_connector(self):
#         return self.mock_object('ProjectConnector')
#
#     def mock_static_kpi(self):
#         static_kpi = self.mock_object('CCBZA_SANDToolBox.get_kpi_static_data')
#         static_kpi.return_value = DataTestUnitCCBZA_SAND.static_data
#         return static_kpi.return_value
#
#     def mock_store_data(self):
#         store_data = self.mock_object('CCBZA_SANDToolBox.get_store_data_by_store_id')
#         store_data.return_value = DataTestUnitCCBZA_SAND.store_data
#         return store_data.return_value
#
#     def mock_template_data(self):
#         template_data_mock = self.mock_object('CCBZA_SANDToolBox.get_template_data')
#         template_data = {}
#         template_path = '{}/Data/Template_L&T.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))
#         print template_path
#         sheet_names = pd.ExcelFile(template_path).sheet_names
#         for sheet in sheet_names:
#             template_data[sheet] = parse_template(template_path, sheet, lower_headers_row_index=0)
#         template_data_mock.return_value = template_data
#         return template_data_mock.return_value
#
#     # def get_tool_box_with_mocked_template_path(self, path):
#     #     # print object.__class__.__name__
#     #     template_path_mock = self.mock_object('CCBZA_SANDToolBox.get_template_path')
#     #     template_path_mock.return_value = path
#     #     self.template_path_mock = template_path_mock.return_value
#     #     self.mock_template_data(self.template_path_mock)
#     #     tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#     #     return tool_box
#
#     # this is for functional test
#     def test_get_store_data_by_store_id_returns_(self):
#         pass
#
#     def test_get_template_path_returns_the_path_to_template_with_template_name_relevant_to_store_type(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         template_path = tool_box.get_template_path()
#         print template_path
#         expected_result = '{}/../Data/Template_L&T.xlsx'.format(os.path.dirname(os.path.realpath(__file__)).replace('Tests', 'Utils'))
#         self.assertEquals(template_path, expected_result)
#
#     def test_get_template_data_returns_template_dict_with_required_tabs_and_necessary_columns(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         template_data = tool_box.get_template_data()
#         # self.assertFalse(template_data['KPI']['Dependancy'].values[0])
#         self.assertTrue(all([tab in template_data.keys() for tab in DataTestUnitCCBZA_SAND.required_template_tabs]))
#         self.assertIsInstance(template_data, dict)
#         for key, sheet_data in template_data.items():
#             self.assertIsInstance(sheet_data, pd.DataFrame)
#             if key == KPI_TAB:
#                 self.assertTrue(all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_kpi_tab))
#             elif key == SURVEY_TAB:
#                 self.assertTrue(all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_survey_tab))
#             else:
#                 pass # add the tests after adding code (to see which columns are really necessary)
#
#     def test_get_template_data_logs_error_when_no_template_corresponding_to_store_type(self):
#         pass #look up later at home
#
#     def test_create_kpi_results_container_returns_empty_dataframe_with_required_columns(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_results_df = tool_box.create_kpi_results_container()
#         self.assertTrue(kpi_results_df.empty)
#         self.assertItemsEqual(kpi_results_df.columns.values, DataTestUnitCCBZA_SAND.columns_kpi_results)
#
#     def test_kpi_sets_property_has_list_of_unique_set_names_from_the_template(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_sets = tool_box.kpi_sets
#         print kpi_sets
#         self.assertItemsEqual(kpi_sets, DataTestUnitCCBZA_SAND.kpi_set_names_from_template)
#
#     def test_toolbox_properties(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         # print tool_box.template_path
#         print tool_box.template_data[KPI_TAB][SET_NAME]
#         # print self.template_mock[KPI_TAB][SET_NAME]
#         # print tool_box.kpi_static_data
#         # print tool_box.new_kpi_static_data
#         print tool_box.store_data
#
#     def test_get_kpi_types_by_kpi_returns_a_list_of_kpi_types(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_data = tool_box.template_data[KPI_TAB][tool_box.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
#         expected_kpi_types = ['Price', 'Survey', 'Availability', 'SOS', 'Count']
#         for index, kpi in kpi_data.iterrows():
#             kpi_types=''
#             if index == 0:
#                 kpi_types = tool_box.get_kpi_types_by_kpi(kpi)
#             self.assertItemsEqual(kpi_types, expected_kpi_types)
#
#     def test_split_string_only_one_divider_type(self):
#         expected_list_after_split = ['Price', 'Survey', 'Availability', 'SOS', 'Count']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_split_by_comma
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#
#     def test_split_string_irregular_dividers(self):
#         expected_list_after_split = ['Price', 'Survey', 'Availability', 'SOS', 'Count']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_split_irregularly
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#
#     def test_split_string_one_value(self):
#         expected_list_after_split = ['Price']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_one_value
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#
#     def test_split_empty_string(self):
#         expected_list_after_split = ['']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_empty_string
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         print kpi_types
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#         self.assertTrue(kpi_types)
#
#     def test_split_name_with_space(self):
#         expected_list_after_split = ['Availability KPI', 'SOS', 'Count']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_name_with_space
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         print kpi_types
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#         self.assertTrue(kpi_types)
#
#     def test_get_atomic_kpis_data_returns_dataframe_with_atomic_kpis_parameters(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_data = tool_box.template_data[KPI_TAB][tool_box.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
#         for index, kpi in kpi_data.iterrows():
#             kpi_types = tool_box.get_kpi_types_by_kpi(kpi)
#             for kpi_type in kpi_types:
#                 atomic_kpis_data = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#                 print kpi_type
#                 if kpi_type == 'Availability':
#                     print atomic_kpis_data
#         # finish test - make sure that the template path is taken from Test data
#         # think how to test template path...
#
#     def test_get_availability_and_price_calculation_parameters(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         df = tool_box.template_data[PRICE_TAB].iloc[2]
#         filters = tool_box.get_availability_and_price_calculation_parameters(df)
#         print filters
#         df = tool_box.template_data[PRICE_TAB].iloc[1]
#         filters = tool_box.get_availability_and_price_calculation_parameters(df)
#         print filters