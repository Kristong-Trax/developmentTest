# from Trax.Utils.Testing.Case import TestCase, MockingTestCase
# from Trax.Data.Testing.SeedNew import Seeder
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from mock import MagicMock
# from Projects.CBCIL.Utils.KPIToolBox import CBCILCBCIL_ToolBox
#
# class TestToolBox(MockingTestCase):
#     seeder = Seeder()
#
#     @property
#     def import_path(self):
#         return 'Projects.CBCIL.Utils.KPIToolBox'
#
#     # mock_path = 'Projects.CBCIL.Utils.KPIToolBox'
#
#     def setUp(self):
#         super(TestToolBox, self).setUp()
#         self._mock_data_provider()
#         self._mock_project_connector = self.mock_object('AwsProjectConnector')
#         # self._main_calculation_mock = self.mock_object('CBCILCBCIL_ToolBox.main_calculation')
#
#
#     def _mock_data_provider(self):
#         self._data_provider = MagicMock()
#         # return self._data_provider
#         self._data_provider_data = {}
#
#         def get_item(key):
#             return self._data_provider_data[key] if key in self._data_provider_data else MagicMock()
#         self._data_provider.__getitem__.side_effect = get_item
#
#
#     # def test_main(self):
#         # print self._main_calculation_mock
#
#     def test_data_provider(self):
#         print self._data_provider
#
#     def test_combine_kpi_details_returns_dictionary_and_expected_key_value_pairs(self):
#         kpi_fk = 1
#         scores = [(100, None), (False, None)]
#         denominator_weight = 0.5
#         # output = MagicMock()
#         # kpi_tool_box = CBCILCBCIL_ToolBox(self._data_provider, output)
#         kpi_details = CBCILCBCIL_ToolBox.combine_kpi_details(kpi_fk, scores, denominator_weight)
#         self.assertIsInstance(kpi_details, dict)
#         self.assertDictEqual(kpi_details, {'denominator_weight': 0.5, 'kpi_fk': 1, 'atomic_scores_and_weights':
#                                            [(100, None), (False, None)]})
#
#     def test_reallocate_weights_to_kpis_with_results_unadjusted_kpis_if_kpis_without_score_empty(self):
#         pass