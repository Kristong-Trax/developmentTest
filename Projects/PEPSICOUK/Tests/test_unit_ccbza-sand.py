#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# import pandas as pd
# from Projects.CCBZA_SAND.Utils.KPIToolBox import CCBZAToolBox
#
#
# __author__ = 'natalyak'
#
#
# class TestCCBZA_SAND(TestCase):
#
#     @mock.patch('Projects.CCBZA_SAND.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'ccbza-sand'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = CCBZAToolBox(self.data_provider_mock, MagicMock())
#
#
# from Trax.Utils.Testing.Case import TestCase, MockingTestCase
# from Trax.Data.Testing.SeedNew import Seeder
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from mock import MagicMock
# from Projects.CCBZA_SAND.Utils.KPIToolBox import CCBZA_SANDToolBox
# from Projects.CCBZA_SAND.Tests.data_test_unit_ccbza_sand import DataTestUnitCCBZA_SAND, DataScores, SCIFDataTestCCBZA_SAND, MatchProdSceneDataTestCCBZA_SAND
# from Projects.CCBZA_SAND.Utils.ParseTemplates import parse_template
# from Trax.Algo.Calculations.Core.DataProvider import Output
# from mock import patch
# import os
# import pandas as pd
# from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
#     SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, TARGET, SKU, POS, OTHER, MAX_SCORE
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
#         # super(TestCCBZA_SAND, self).setUp()
#         super(TestCCBZA_SAND, self).set_up()
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
#         # self.common_mock = self.mock_common()
#         self.mock_common_project_connector_mock = self.mock_common_project_connector()
#         self.ps_dataprovider_project_connector_mock = self.mock_ps_data_provider_project_connector()
#         self.session_info_mock = self.mock_session_info()
#         self.store_assortment_mock = self.mock_store_assortment()
#
#         self.kpi_result_values_mock = self.mock_kpi_result_value_table()
#         self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
#
#     def mock_data_provider(self):
#         self.data_provider_mock = MagicMock()
#         # return self._data_provider
#         self.data_provider_data_mock = {}
#         def get_item(key):
#             return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()
#         self.data_provider_mock.__getitem__.side_effect = get_item
#
#     def mock_ps_data_provider_project_connector(self):
#         return self.mock_object('PsDataProvider.rds_connection', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
#
#     def mock_session_info(self):
#         return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')
#
#     def mock_store_assortment(self):
#         return self.mock_object('PsDataProvider.get_store_assortment', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
#
#     def mock_project_connector(self):
#         return self.mock_object('ProjectConnector')
#
#     def mock_common_project_connector(self):
#         return self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.CommonV2')
#
#     def mock_common(self):
#         return self.mock_object('Common', path='KPIUtils_v2.DB.CommonV2')
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
#         template_path = '{}/Data/Template_L&T_test_updated.xlsx'.format(os.path.dirname(os.path.realpath(__file__)))
#         print template_path
#         sheet_names = pd.ExcelFile(template_path).sheet_names
#         for sheet in sheet_names:
#             template_data[sheet] = parse_template(template_path, sheet, lower_headers_row_index=0)
#         template_data_mock.return_value = template_data
#         return template_data_mock.return_value
#
#     def mock_survey_answer(self, answer):
#         survey_answer = self.mock_object('CCBZA_SAND_GENERALToolBox.get_survey_answer')
#         survey_answer.return_value = answer
#
#     def mock_scene_item_facts(self, data):
#         self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)
#
#     def mock_match_product_in_scene(self, data):
#         self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)
#
#     def mock_scene_kpi_results(self, data):
#         scene_data = self.mock_object('PsDataProvider.get_scene_results', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
#         scene_data.return_value = data
#
#     def mock_kpi_result_value_table(self):
#         kpi_result_value = self.mock_object('CCBZA_SANDToolBox.get_kpi_result_values_df')
#         kpi_result_value.return_value = DataTestUnitCCBZA_SAND.kpi_results_values_table
#         return kpi_result_value.return_value
#
#     def mock_kpi_score_value_table(self):
#         kpi_score_value = self.mock_object('CCBZA_SANDToolBox.get_kpi_result_values_df')
#         kpi_score_value.return_value = DataTestUnitCCBZA_SAND.kpi_scores_values_table
#         return kpi_score_value.return_value
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
#             elif key == PRICE_TAB:
#                 self.assertTrue(
#                     all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_price_tab))
#             elif key == AVAILABILITY_TAB:
#                 self.assertTrue(
#                     all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_avaialability_tab))
#             elif key == SOS_TAB:
#                 self.assertTrue(
#                     all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_sos_tab))
#             elif key == COUNT_TAB:
#                 self.assertTrue(
#                     all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_count_tab))
#             elif key == PLANOGRAM_TAB:
#                 self.assertTrue(
#                     all(col in sheet_data.columns.values for col in DataTestUnitCCBZA_SAND.columns_planogram_tab))
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
#     def test_scif_property_of_tool_box_returns_scif_filtered_by_manufacturer(self): # not relevant - to be deleted
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         scif = tool_box.scif
#         expected_pk_list = [1, 2, 3, 4, 5, 6, 9, 10, 11, 14, 15, 17]
#         self.assertIsInstance(scif, pd.DataFrame)
#         self.assertItemsEqual(expected_pk_list, scif['pk'].values)
#
#     def test_scif_property_of_tool_box_returns_empty_scif_if_no_products_by_manufacturer(self): # not relevant - to be deleted
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_no_manufacturer)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         scif = tool_box.scif
#         self.assertTrue(scif.empty)
#
#     def test_toolbox_properties(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         self.mock_scene_kpi_results(DataTestUnitCCBZA_SAND.scene_kpi_results_1)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         # print tool_box.kpi_result_values
#         # print tool_box.scene_kpi_results
#         print tool_box.template_data['KPI']
#         # self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         # print tool_box.scif
#         # print tool_box.scif_KO_only
#         # print tool_box.template_path
#         # print tool_box.template_data[KPI_TAB][SET_NAME]
#         # print self.template_mock[KPI_TAB][SET_NAME]
#         # print tool_box.kpi_static_data
#         # print tool_box.new_kpi_static_data
#         # self.mock_survey_answer('Yes')
#         # print tool_box.tools.get_survey_answer()
#         # self.data_provider_mock['scene_item_facts'].return_value = SCIFDataTestCCBZA_SAND.scif_for_filtering
#         # print tool_box.scif
#
#     def test_get_kpi_types_by_kpi_returns_a_list_of_kpi_types_if_kpi_types_exist(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_data = tool_box.template_data[KPI_TAB][tool_box.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
#         expected_kpi_types = ['Price', 'Survey', 'Availability', 'SOS', 'Count']
#         for index, kpi in kpi_data.iterrows():
#             kpi_types = ''
#             if index == 0:
#                 kpi_types = tool_box.get_kpi_types_by_kpi(kpi)
#             self.assertItemsEqual(kpi_types, expected_kpi_types)
#             self.assertIsInstance(kpi_types, list)
#
#     def test_get_kpi_types_by_kpi_returns_empty_list_if_no_kpi_types(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi = DataTestUnitCCBZA_SAND.avail_and_pricing_all_bonus_kpi_series
#         kpi_types = tool_box.get_kpi_types_by_kpi(kpi)
#         self.assertFalse(kpi_types)
#         self.assertIsInstance(kpi_types, list)
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
#         expected_list_after_split = []
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_empty_string
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#         self.assertFalse(kpi_types)
#
#     def test_split_name_with_space(self):
#         expected_list_after_split = ['Availability KPI', 'SOS', 'Count']
#         string_to_split = DataTestUnitCCBZA_SAND.kpi_types_name_with_space
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#         self.assertTrue(kpi_types)
#
#     def test_split_string_number(self):
#         expected_list_after_split = ['200']
#         string_to_split = DataTestUnitCCBZA_SAND.string_represented_by_number
#         kpi_types = CCBZA_SANDToolBox.split_and_strip(string_to_split)
#         self.assertItemsEqual(kpi_types, expected_list_after_split)
#         self.assertIsInstance(kpi_types, list)
#
#     def test_string_or_list_returns_string_if_one_value_in_string(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         expected_result = 'Price'
#         input_value = DataTestUnitCCBZA_SAND.kpi_types_one_value
#         output_value = tool_box.string_or_list(input_value)
#         self.assertEquals(expected_result, output_value)
#         self.assertIsInstance(output_value, str)
#
#     def test_string_or_list_returns_trimmed_list_if_values_separated_by_comma(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         expected_list = ['Price', 'Survey', 'Availability', 'SOS', 'Count']
#         input_value = DataTestUnitCCBZA_SAND.kpi_types_split_by_comma
#         output_value = tool_box.string_or_list(input_value)
#         self.assertEquals(expected_list, output_value)
#         self.assertIsInstance(output_value, list)
#
#     def test_string_or_list_returns_empty_list_if_empty_input_string(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         input_value = DataTestUnitCCBZA_SAND.kpi_types_empty_string
#         output_value = tool_box.string_or_list(input_value)
#         self.assertFalse(output_value)
#         self.assertIsInstance(output_value, list)
#
#     def test_get_string_or_number_returns_number_if_numeric_field(self):
#         field, input_value = 'size', '200'
#         output_value = CCBZA_SANDToolBox.get_string_or_number(field, input_value)
#         expected_result = 200.0
#         self.assertEquals(expected_result, output_value)
#         self.assertIsInstance(output_value, float)
#
#     def test_get_string_or_number_returns_string_if_numeric_field_but_non_numeric_value(self):
#         field, input_value = 'size', '/200/'
#         output_value = CCBZA_SANDToolBox.get_string_or_number(field, input_value)
#         expected_result = '/200/'
#         self.assertEquals(expected_result, output_value)
#         self.assertIsInstance(output_value, str)
#
#     def test_get_string_or_number_returns_string_if_non_numeric_field_numeric_value(self):
#         field, input_value = 'pack', '200'
#         output_value = CCBZA_SANDToolBox.get_string_or_number(field, input_value)
#         expected_result = '200'
#         self.assertEquals(expected_result, output_value)
#         self.assertIsInstance(output_value, str)
#
#     def test_get_atomic_kpis_data_returns_dataframe_with_atomic_kpis_parameters(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_data = tool_box.template_data[KPI_TAB][tool_box.template_data[KPI_TAB][SET_NAME] == kpi_set_name]
#         for index, kpi in kpi_data.iterrows():
#             print type(kpi)
#             print kpi
#             kpi_types = tool_box.get_kpi_types_by_kpi(kpi)
#             for kpi_type in kpi_types:
#                 atomic_kpis_data = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#                 print kpi_type
#                 if kpi_type == 'Availability':
#                     pass
#                     # print atomic_kpis_data
#         # finish test - make sure that the template path is taken from Test data
#         # think how to test template path...
#
#     def test_get_atomic_kpis_data_returns_dataframe_with_relevant_atomic_kpis(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_type = 'Survey'
#         kpi = DataTestUnitCCBZA_SAND.test_kpi_1_series
#         expected_atomics = ['Atomic KPI 1', 'Atomic KPI 3', 'Atomic KPI 4', 'Atomic KPI 5', 'Atomic KPI 6',
#                             'Atomic KPI 7', 'Atomic KPI 8', 'Atomic KPI 9', 'Atomic KPI 15']
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         self.assertItemsEqual(expected_atomics, atomic_kpis[ATOMIC_KPI_NAME].values)
#         self.assertIsInstance(atomic_kpis, pd.DataFrame)
#
#     def test_get_atomic_kpis_data_returns_empty_dataframe_if_no_atomic_kpis_of_type(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         kpi_type = 'Count'
#         kpi = DataTestUnitCCBZA_SAND.test_kpi_1_series
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         self.assertTrue(atomic_kpis.empty)
#
#     def test_does_kpi_have_split_score_returns_true_if_y(self):
#         kpi = DataTestUnitCCBZA_SAND.test_kpi_1_series
#         is_split_score = CCBZA_SANDToolBox.does_kpi_have_split_score(kpi)
#         self.assertTrue(is_split_score)
#
#     def test_does_kpi_have_split_score_returns_false_if_n(self):
#         kpi = DataTestUnitCCBZA_SAND.avail_and_pricing_all_bonus_kpi_series
#         is_split_score = CCBZA_SANDToolBox.does_kpi_have_split_score(kpi)
#         self.assertFalse(is_split_score)
#
#     def test_does_kpi_have_split_score_logs_error_if_neither_y_or_n(self):
#         pass # finish - look up at home
#
#     def test_calculate_survey_adds_appropriate_score_to_results_container_when_survey_answer_is_yes(self):
#         self.mock_survey_answer('Yes')
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         tool_box.current_kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_type = 'Survey'
#         kpi = DataTestUnitCCBZA_SAND.coolers_kpi_series
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         tool_box.calculate_survey(atomic_kpis)
#         atomics_list = atomic_kpis[ATOMIC_KPI_NAME].values
#         atomic_scores_list = map(lambda x: float(x), atomic_kpis[SCORE].values)
#         max_score_list = atomic_scores_list
#         print atomic_scores_list
#         for i in xrange(len(tool_box.kpi_results_data)):
#             result = tool_box.kpi_results_data.iloc[i]
#             self.assertEquals(result[SET_NAME], 'COOLERS & MERCHANDISING')
#             self.assertEquals(result[KPI_NAME], kpi[KPI_NAME])
#             self.assertEquals(result[ATOMIC_KPI_NAME], atomics_list[i])
#             self.assertEquals(result[SCORE], atomic_scores_list[i])
#             self.assertEquals(result[MAX_SCORE], max_score_list[i])
#
#         # self.assertEquals(tool_box.kpi_results_data[SET_NAME].values[0], 'COOLERS & MERCHANDISING')
#         # self.assertEquals(tool_box.kpi_results_data[KPI_NAME].values[0], 'Coolers')
#         # self.assertEquals(tool_box.kpi_results_data[ATOMIC_KPI_NAME].values[0], 'Cooler Outside or 1st Inside')
#         # self.assertEquals(tool_box.kpi_results_data[SCORE].values[0], 10)
#
#     def test_calculate_survey_adds_zero_score_to_results_container_when_survey_answer_is_no(self):
#         self.mock_survey_answer('No')
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         tool_box.current_kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_type = 'Survey'
#         kpi = DataTestUnitCCBZA_SAND.coolers_kpi_series
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         tool_box.calculate_survey(atomic_kpis)
#         atomics_list = atomic_kpis[ATOMIC_KPI_NAME].values
#         atomic_scores_list = [0]
#         max_score_list = [10]
#         for i in xrange(len(tool_box.kpi_results_data)):
#             result = tool_box.kpi_results_data.iloc[i]
#             self.assertEquals(result[SET_NAME], 'COOLERS & MERCHANDISING')
#             self.assertEquals(result[KPI_NAME], kpi[KPI_NAME])
#             self.assertEquals(result[ATOMIC_KPI_NAME], atomics_list[i])
#             self.assertEquals(result[SCORE], atomic_scores_list[i])
#
#         # self.assertEquals(tool_box.kpi_results_data[SET_NAME].values[0], 'COOLERS & MERCHANDISING')
#         # self.assertEquals(tool_box.kpi_results_data[KPI_NAME].values[0], 'Coolers')
#         # self.assertEquals(tool_box.kpi_results_data[ATOMIC_KPI_NAME].values[0], 'Cooler Outside or 1st Inside')
#         # self.assertEquals(tool_box.kpi_results_data[SCORE].values[0], 0)
#
#     def test_get_general_calculation_parameters_returns_dict_with_scenes_related_to_templates_and_manufacturer_KO(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.count_atomic_series
#         general_filters = tool_box.get_general_calculation_parameters(atomic_kpi)
#         expected_return_value = {'scene_fk': [95, 96], 'manufacturer_name': 'KO PRODUCTS'}
#         self.assertDictEqual(general_filters, expected_return_value)
#
#     def test_get_general_calculation_parameters_returns_dict_with_all_scenes_in_session_if_no_template_names_manuf_not_KO(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.count_atomic_template_field_empty
#         general_filters = tool_box.get_general_calculation_parameters(atomic_kpi)
#         expected_return_value = {'scene_fk': [95, 96, 97]}
#         self.assertDictEqual(general_filters, expected_return_value)
#
#     def test_get_general_calculation_parameters_returns_dict_with_all_scenes_in_session_if_no_template_field_no_KO_field(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.survey_atomic_series
#         general_filters = tool_box.get_general_calculation_parameters(atomic_kpi)
#         expected_return_value = {'scene_fk': [95, 96, 97]}
#         self.assertDictEqual(general_filters, expected_return_value)
#
#     def test_get_general_calculation_parameters_returns_dict_with_scenes_and_product_types_if_product_type_parameter(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.count_atomic_series
#         general_filters = tool_box.get_general_calculation_parameters(atomic_kpi, product_types=[SKU, OTHER])
#         expected_result = {'scene_fk': [95, 96], 'product_type': ['SKU', 'Other']}
#         self.assertDictEqual(expected_result, general_filters)
#         print general_filters
#
#     def test_get_availability_and_price_calculation_parameters_returns_a_dict_with_type_as_key_and_value_as_value(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.price_atomic_series
#         filters = tool_box.get_availability_and_price_calculation_parameters(atomic_kpi)
#         expected_result = {'form_factor': 'can', 'brand_name': ['FANTA ORANGE', 'COCA COLA', 'SPRITE', 'STONEY'], 'size': 200.0}
#         self.assertDictEqual(expected_result, filters)
#         # print filters
#
#     def test_get_availability_and_price_calculation_parameters_returns_a_dict_values_as_floats_numeric_columns(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.price_atomic_series_diffr_sizes
#         filters = tool_box.get_availability_and_price_calculation_parameters(atomic_kpi)
#         expected_result = {'form_factor': 'can', 'brand_name': ['FANTA ORANGE', 'COCA COLA', 'SPRITE', 'STONEY'], 'size': [200.0, 150.0]}
#         self.assertDictEqual(expected_result, filters)
#
#     def test_get_availability_and_price_calculation_parameters_returns_type_values_when_some_are_missing(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.price_atomic_series_one_type_value
#         filters = tool_box.get_availability_and_price_calculation_parameters(atomic_kpi)
#         expected_result = {'brand_name': ['FANTA ORANGE', 'COCA COLA', 'SPRITE', 'STONEY']}
#         self.assertDictEqual(expected_result, filters)
#
#     def test_get_availability_and_price_calculation_parameters_returns_type_values_when_some_are_missing_in_between(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.price_atomic_series_missing_between
#         filters = tool_box.get_availability_and_price_calculation_parameters(atomic_kpi)
#         expected_result = {'product_ean_code': ['3434443', '232323'], 'form_factor': 'can'}
#         self.assertDictEqual(expected_result, filters)
#
#     def test_get_availability_and_price_calculation_parameters_returns_empty_dict_when_no_type_values_in_template(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.price_atomic_series_no_type_values
#         filters = tool_box.get_availability_and_price_calculation_parameters(atomic_kpi)
#         self.assertIsInstance(filters, dict)
#         self.assertFalse(filters)
#
#     def test_get_availability_and_price_calculation_parameters_logs_error_if_value_corresponding_to_type(self):
#         pass
#         # look up logger later
#
#     def test_calculate_count_adds_relevant_score_to_results_container_where_conditions_are_met(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         tool_box.current_kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_type = 'Count'
#         kpi = DataTestUnitCCBZA_SAND.coolers_kpi_series
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         atomics_list = atomic_kpis[ATOMIC_KPI_NAME].values
#         expected_atomic_scores_list = [10]
#         max_score_list = [10]
#         tool_box.calculate_count(atomic_kpis)
#         for i in xrange(len(tool_box.kpi_results_data)):
#             result = tool_box.kpi_results_data.iloc[i]
#             self.assertEquals(result[SET_NAME], 'COOLERS & MERCHANDISING')
#             self.assertEquals(result[KPI_NAME], kpi[KPI_NAME])
#             self.assertEquals(result[ATOMIC_KPI_NAME], atomics_list[i])
#             self.assertEquals(result[SCORE], expected_atomic_scores_list[i])
#             self.assertEquals(result[MAX_SCORE], max_score_list[i])
#
#     def test_calculate_count_adds_relevant_score_to_results_container_where_conditions_are_not_met(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_scif_for_filtering_less_bays)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         tool_box.current_kpi_set_name = 'COOLERS & MERCHANDISING'
#         kpi_type = 'Count'
#         kpi = DataTestUnitCCBZA_SAND.coolers_kpi_series
#         atomic_kpis = tool_box.get_atomic_kpis_data(kpi_type, kpi)
#         atomics_list = atomic_kpis[ATOMIC_KPI_NAME].values
#         expected_atomic_scores_list = [0]
#         tool_box.calculate_count(atomic_kpis)
#         max_score_list = [10]
#         # print tool_box.kpi_results_data
#         for i in xrange(len(tool_box.kpi_results_data)):
#             result = tool_box.kpi_results_data.iloc[i]
#             self.assertEquals(result[SET_NAME], 'COOLERS & MERCHANDISING')
#             self.assertEquals(result[KPI_NAME], kpi[KPI_NAME])
#             self.assertEquals(result[ATOMIC_KPI_NAME], atomics_list[i])
#             self.assertEquals(result[SCORE], expected_atomic_scores_list[i])
#             self.assertEquals(result[MAX_SCORE], max_score_list[i])
#
#     def test_get_sos_calculation_parameters_two_conditions(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.sos_atomic_series
#         filters = tool_box.get_sos_calculation_parameters(atomic_kpi)
#         print filters
#         expected_result = {'Condition 1': {'denom': {'Category': 'SSD'},
#                                            'numer': {'Category': 'SSD', 'Attribute 2': 'Quad Cola'},
#                                            'target': '50'},
#                            'Condition 2': {'denom': {'Category': 'SSD'},
#                                            'numer': {'Attribute 3': 'Diets','Category': 'SSD'},
#                                            'target': '30'}}
#         self.assertDictEqual(filters, expected_result)
#
#     def testget_sos_calculation_parameters_one_condition(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         atomic_kpi = DataTestUnitCCBZA_SAND.sos_atomic_series_one_condition
#         filters = tool_box.get_sos_calculation_parameters(atomic_kpi)
#         expected_result = {'Condition 1': {'numer': {'product_ean_code': ['5449000234612', '5449000027559',
#                                                                           '5449000234636'],
#                                                      'category': 'SSD'},
#                                            'denom': {'category': 'SSD'},
#                                            'target': '65'}}
#         self.assertDictEqual(filters, expected_result)
#
#     def test_calculate_price_presence_where_there_is_at_least_one_price_per_sku(self):
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_price_presence)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         price_filters = {'scene_fk': 97, 'product_fk': [3, 11, 12, 13]}
#         matches = tool_box.match_product_in_scene.copy()
#         relevant_matches = matches[tool_box.tools.get_filter_condition(matches, **price_filters)]
#         price_presence_result = tool_box.calculate_price_presence(relevant_matches, price_filters['product_fk'])
#         expected_result = 100
#         self.assertEquals(price_presence_result, expected_result)
#
#     def test_calculate_price_presence_where_one_sku_has_no_prices(self):
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_price_presence)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         price_filters = {'scene_fk': 95, 'product_fk': [1, 2, 3, 4, 5, 6, 7, 8]}
#         matches = tool_box.match_product_in_scene.copy()
#         relevant_matches = matches[tool_box.tools.get_filter_condition(matches, **price_filters)]
#         price_presence_result = tool_box.calculate_price_presence(relevant_matches, price_filters['product_fk'])
#         expected_result = 0
#         self.assertEquals(price_presence_result, expected_result)
#
#     def test_calculate_price_vs_target_where_at_least_one_price_below_target(self):
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_price_presence)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         price_filters = {'scene_fk': 96, 'product_fk': [1, 9, 10, 7, 13]}
#         matches = tool_box.match_product_in_scene.copy()
#         relevant_matches = matches[tool_box.tools.get_filter_condition(matches, **price_filters)]
#         target = 7
#         price_vs_target_result = tool_box.calculate_price_vs_target(relevant_matches, price_filters['product_fk'], target)
#         expected_result = 100
#         self.assertEquals(price_vs_target_result, expected_result)
#
#     def test_calculate_price_vs_target_where_one_sku_does_not_meet_target(self):
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_price_presence)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         price_filters = {'scene_fk': 98, 'product_fk': [1, 9, 10, 7, 13]}
#         matches = tool_box.match_product_in_scene.copy()
#         relevant_matches = matches[tool_box.tools.get_filter_condition(matches, **price_filters)]
#         target = 8
#         price_vs_target_result = tool_box.calculate_price_vs_target(relevant_matches, price_filters['product_fk'], target)
#         expected_result = 0
#         self.assertEquals(price_vs_target_result, expected_result)
#
#
#     # def test_get_availability_and_price_calculation_parameters(self):
#     #     tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#     #     df = tool_box.template_data[PRICE_TAB].iloc[2]
#     #     filters = tool_box.get_availability_and_price_calculation_parameters(df)
#     #     print filters
#     #     df = tool_box.template_data[PRICE_TAB].iloc[1]
#     #     filters = tool_box.get_availability_and_price_calculation_parameters(df)
#     #     print filters
#     #
#
#     def test_get_kpi_result_value_pk_by_value_returns_pk_where_value_exists(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         pk = tool_box.get_kpi_result_value_pk_by_value('Passed')
#         expected_result = 1
#         self.assertEquals(pk, expected_result)
#
#     def test_get_kpi_result_value_pk_by_value_returns_None_where_value_does_not_exist(self):
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         pk = tool_box.get_kpi_result_value_pk_by_value('Pass')
#         self.assertIsNone(pk)
#         # add log test!
#
#     def test_merge_scif_match_prod_in_scene(self):
#         self.mock_scene_item_facts(SCIFDataTestCCBZA_SAND.scif_for_filtering)
#         self.mock_match_product_in_scene(MatchProdSceneDataTestCCBZA_SAND.matches_scif_for_filtering)
#         tool_box = CCBZA_SANDToolBox(self.data_provider_mock, self.output)
#         merged = tool_box.match_product_in_scene.merge(tool_box.scif, left_on='product_fk', right_on='item_id', how='left')
#         print merged[['product_fk', 'item_id', 'product_name']]
