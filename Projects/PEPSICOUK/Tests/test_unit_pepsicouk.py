#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# import pandas as pd
# from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKToolBox
#
#
# __author__ = 'natalyak'
#
#
# class TestPEPSICOUK(TestCase):
#
#     @mock.patch('Projects.PEPSICOUK.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'pepsicouk'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = PEPSICOUKToolBox(self.data_provider_mock, MagicMock())

from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from Trax.Data.Testing.SeedNew import Seeder
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from mock import MagicMock
from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKToolBox
from Projects.PEPSICOUK.Tests.data_test_unit_pepsicouk import DataTestUnitPEPSICOUK, DataScores
from Trax.Algo.Calculations.Core.DataProvider import Output
from mock import patch
import os
import pandas as pd

__author__ = 'natalya'


def get_exclusion_template_df_all_tests():
    template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
    return template_df


class Test_PEPSICOUK(MockingTestCase):
    # seeder = Seeder()
    template_df_mock = get_exclusion_template_df_all_tests()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.Utils.KPIToolBox'

    def set_up(self):
        super(Test_PEPSICOUK, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        self.mock_various_project_connectors()
        self.project_connector_mock = self.mock_project_connector()

        self.ps_dataprovider_project_connector_mock = self.mock_ps_data_provider_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        self.full_store_data_mock = self.mock_store_data()

        # self.probe_groups_mock = self.mock_probe_groups()
        self.custom_entity_data_mock = self.mock_custom_entity_data()
        self.on_display_products_mock = self.mock_on_display_products()

        self.exclusion_template_mock = self.mock_template_data()
        self.output = MagicMock()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        # self.assortment_mock = self.mock_assortment_object()
        self.lvl3_ass_result_mock = self.mock_lvl3_ass_result()
        self.mock_all_products()
        self.mock_all_templates()
        self.mock_position_graph()

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_lvl3_ass_result(self):
        probe_group = self.mock_object('Assortment.calculate_lvl3_assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')
        probe_group.return_value = DataTestUnitPEPSICOUK.test_case_1_ass_result
        return probe_group.return_value

    # @classmethod
    # def setUpClass(cls):
    #     """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
    #         and store the result as class variable
    #     """
    #     super(Test_PEPSICOUK, cls).setUpClass()
    #     cls.template_df_mock = cls.get_exclusion_template_df_all_tests()
    #
    # @classmethod
    # def get_exclusion_template_df_all_tests(cls):
    #     template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
    #     return template_df

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
                                                                     sheetname='all_products')

    def mock_all_templates(self):
        self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    def mock_scene_info(self, data):
        self.data_provider_data_mock['scenes_info'] = data.where(data.notnull(), None)

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider'), self.mock_object('DbUsers')
        # self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        # self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        # self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')

        # KPIUtils_v2 / DB / PsProjectConnector
        # self.mock_object('DbUsers', path='KPIUtils_v2.DB.PsProjectConnector'), self.mock_object('DbUsers')

    def mock_on_display_products(self):
        on_display_products = self.mock_object('PEPSICOUKCommonToolBox.get_on_display_products',
                                               path='Projects.PEPSICOUK.Utils.CommonToolBox')
        on_display_products.return_value = DataTestUnitPEPSICOUK.on_display_products
        return on_display_products.return_value

    def mock_assortment_object(self):
        return self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_external_targets_data(self):
        external_targets_df = pd.read_excel(DataTestUnitPEPSICOUK.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_probe_groups(self):
        probe_groups_df = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='stitch_groups')
        probe_groups = self.mock_object('PEPSICOUKToolBox.get_probe_group',
                                        value=probe_groups_df)
        return probe_groups.return_value

    def mock_probe_group_for_particular_test_case(self, data):
        probe_group = self.mock_object('PEPSICOUKSceneToolBox.get_probe_group')
        probe_group.return_value = data.where(data.notnull(), None)
        return probe_group.return_value

    def mock_custom_entity_data(self):
        custom_entities = self.mock_object('PEPSICOUKCommonToolBox.get_custom_entity_data',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox')
        custom_entities.return_value = DataTestUnitPEPSICOUK.custom_entity
        return custom_entities.return_value

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitPEPSICOUK.kpi_static_data
        return static_kpi.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_ps_data_provider_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    # def mock_static_kpi(self):
    #     static_kpi = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_static_data',
    #                                   value=DataTestUnitPEPSICOUK.kpi_static_data,
    #                                   path='Projects.PEPSICOUK.Utils.CommonToolBox')
    #     # static_kpi.return_value = DataTestUnitCCBZA_SAND.static_data
    #     return static_kpi.return_value

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_template_data(self):
        # template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
        template_df = Test_PEPSICOUK.template_df_mock
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        kpi_result_value.return_value = DataTestUnitPEPSICOUK.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_kpi_score_value_table(self):
        kpi_score_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox',)
        kpi_score_value.return_value = DataTestUnitPEPSICOUK.kpi_scores_values_table
        return kpi_score_value.return_value

    def mock_scene_kpi_results(self, data):
        scene_results = self.mock_object('PsDataProvider.get_scene_results', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        scene_results.return_value = data
        return

    # def test_whatever(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
    #     print tool_box.exclusion_template
    #     print tool_box.lvl3_ass_result
    #     print tool_box.scene_kpi_results
    #     print tool_box.scene_info
    #
    # def test_assortment(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
    #     # tool_box.calculate_assortment() # complete mock data later
    #     print tool_box.kpi_results

    def test_calculate_hero_shelf_placement_horizontal(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_shelf_placement_hero_skus()
        expected_list = []
        expected_list.append({'kpi_fk': 311, 'numerator': 1, 'result': round(7.0 / 12, 5)})
        expected_list.append({'kpi_fk': 312, 'numerator': 1, 'result': round(3.0 / 12, 5)})
        expected_list.append({'kpi_fk': 314, 'numerator': 1, 'result': round(2.0 / 12, 5)})
        expected_list.append({'kpi_fk': 314, 'numerator': 2, 'result': round(2.0 / 12, 5)})
        expected_list.append({'kpi_fk': 313, 'numerator': 2, 'result': round(2.0 / 12, 5)})
        expected_list.append({'kpi_fk': 312, 'numerator': 2, 'result': round(8.0 / 12, 5)})
        expected_list.append({'kpi_fk': 310, 'numerator': 1, 'result': 1})
        expected_list.append({'kpi_fk': 310, 'numerator': 2, 'result': 1})
        expected_list.append({'kpi_fk': 309, 'numerator': 2, 'result': 2})

        kpi_results = tool_box.kpi_results
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_brand_full_bay_if_relevant_scenes_exist_in_session(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        # self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        # self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_brand_full_bay()
        expected_list = list()
        expected_list.append({'kpi_fk': 316, 'numerator': 167, 'score': 0})
        expected_list.append({'kpi_fk': 327, 'numerator': 167, 'score': 1})
        expected_list.append({'kpi_fk': 316, 'numerator': 168, 'score': 0})
        expected_list.append({'kpi_fk': 327, 'numerator': 168, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertTrue(len(tool_box.kpi_results), 4)

    def test_calculate_brand_full_bay_calculates_no_kpi_results_if_no_relevant_scenes_in_session(self):
        matches, scif = self.create_scif_matches_data_mocks_selected_scenes(DataTestUnitPEPSICOUK.test_case_1, [3])
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_brand_full_bay()
        self.assertTrue(tool_box.kpi_results.empty)
        self.assertFalse(tool_box.match_product_in_scene.empty)

    def test_calculate_hero_sku_stacking_by_sequence_number(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_hero_sku_stacking_by_sequence_number()
        expected_skus_in_results = [1, 2]
        self.assertItemsEqual(tool_box.kpi_results['numerator'].unique().tolist(), expected_skus_in_results)
        self.assertEquals(len(tool_box.kpi_results), 2)
        expected_list = list()
        expected_list.append({'kpi_fk': 315, 'numerator': 1, 'result': 1, 'score': 1})
        expected_list.append({'kpi_fk': 315, 'numerator': 2, 'result': 1, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_hero_sku_stacking_by_sequence_number_returns_zero_results_if_no_stacked_products(self):
        matches, scif = self.create_scif_matches_data_mocks_selected_scenes(DataTestUnitPEPSICOUK.test_case_1, [2])
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_hero_sku_stacking_by_sequence_number()
        expected_skus_in_results = [1, 2]
        self.assertItemsEqual(tool_box.kpi_results['numerator'].unique().tolist(), expected_skus_in_results)
        self.assertEquals(len(tool_box.kpi_results), 2)
        expected_list = list()
        expected_list.append({'kpi_fk': 315, 'numerator': 1, 'result': 0, 'score': 0})
        expected_list.append({'kpi_fk': 315, 'numerator': 2, 'result': 0, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_hero_sku_information_kpis_returns_correct_price_info(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_hero_sku_information_kpis()
        expected_skus_in_results = [1, 2]
        self.assertItemsEqual(tool_box.kpi_results['numerator'].unique().tolist(), expected_skus_in_results)
        self.assertEquals(len(tool_box.kpi_results), 4)
        expected_list = list()
        expected_list.append({'kpi_fk': 317, 'numerator': 1, 'result': 9})
        expected_list.append({'kpi_fk': 318, 'numerator': 1, 'result': 1})
        expected_list.append({'kpi_fk': 317, 'numerator': 2, 'result': -1})
        expected_list.append({'kpi_fk': 318, 'numerator': 2, 'result': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_hero_sku_stacking_width(self):
        matches, scif = self.create_scif_matches_data_mocks_selected_scenes(DataTestUnitPEPSICOUK.test_case_1, [1])
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        sku = 1
        kpi_fk = 315
        tool_box.calculate_hero_sku_stacking_width(sku, kpi_fk)
        expected_list = list()
        expected_list.append({'kpi_fk': 315, 'numerator': 1, 'result': 1, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_hero_sku_stacking_layer_more_than_one_if_sku_stacked(self):
        matches, scif = self.create_scif_matches_data_mocks_selected_scenes(DataTestUnitPEPSICOUK.test_case_1, [1])
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        sku = 2
        kpi_fk = 315
        tool_box.calculate_hero_sku_stacking_layer_more_than_one(sku, kpi_fk)
        expected_list = list()
        expected_list.append({'kpi_fk': 315, 'numerator': 2, 'result': 1, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_hero_sku_stacking_layer_more_than_one_if_sku_not_stacked(self):
        matches, scif = self.create_scif_matches_data_mocks_selected_scenes(DataTestUnitPEPSICOUK.test_case_1, [2])
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        sku = 2
        kpi_fk = 315
        tool_box.calculate_hero_sku_stacking_layer_more_than_one(sku, kpi_fk)
        expected_list = list()
        expected_list.append({'kpi_fk': 315, 'numerator': 2, 'result': 0, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(tool_box.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    @staticmethod
    def check_kpi_results(kpi_results_df, expected_results_dict):
        column = []
        expression = []
        condition = []
        for key, value in expected_results_dict.items():
            column.append(key)
            expression.append('==')
            condition.append(value)
        query = ' & '.join('{} {} {}'.format(i, j, k) for i, j, k in zip(column, expression, condition))
        filtered_df = kpi_results_df.query(query)
        return len(filtered_df)

    def create_scif_matches_data_mocks_selected_scenes(self, test_case_file_path, scene_numbers):
        scif_test_case = pd.read_excel(test_case_file_path, sheetname='scif')
        matches_test_case = pd.read_excel(test_case_file_path, sheetname='matches')
        scif_scene = scif_test_case[scif_test_case['scene_fk'].isin(scene_numbers)]
        matches_scene = matches_test_case[matches_test_case['scene_fk'].isin(scene_numbers)]
        self.mock_scene_item_facts(scif_scene)
        self.mock_match_product_in_scene(matches_scene)
        return matches_scene, scif_scene

    def test_get_relevant_sos_vs_target_kpi_targets_get_relevan_rows_from_kpi_external_targets(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        targets = tool_box.get_relevant_sos_vs_target_kpi_targets()
        expected_list_of_target_rows = [21, 22, 23, 24, 25]
        self.assertItemsEqual(expected_list_of_target_rows, targets['pk'].values.tolist())

    def test_get_relevant_sos_vs_target_kpi_targets_get_relevant_rows_for_brand_vs_brand(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        targets = tool_box.get_relevant_sos_vs_target_kpi_targets(brand_vs_brand=True)
        expected_list_of_target_rows = [28, 29]
        self.assertItemsEqual(expected_list_of_target_rows, targets['pk'].values.tolist())

    def test_sos_brand_vs_brand(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_linear_brand_vs_brand_index()
        expected_list = list()
        expected_list.append({'kpi_fk': 301, 'numerator': 194, 'denominator': 183, 'score': 0})
        expected_list.append({'kpi_fk': 302, 'numerator': 136, 'denominator': 189, 'score':
                                                                                        round((float(60)/195)/(float(135)/195), 5)})
        expected_list.append({'kpi_fk': 303, 'numerator': 2, 'score': 1})
        kpi_results = tool_box.kpi_results
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKToolBox(self.data_provider_mock, self.output)
        tool_box.calculate_sos_vs_target_kpis()
        expected_list = list()
        expected_list.append({'kpi_fk': 296, 'numerator': 2, 'denominator': 11, 'result': round(float(135)/float(255), 5),
                              'score': round(float(135)/float(255)/0.9, 5)})
        expected_list.append({'kpi_fk': 294, 'numerator': 10, 'denominator': 2, 'result': round(float(120)/float(435), 5), 'score':
            round(float(120) / float(435)/0.02, 5)})
        expected_list.append({'kpi_fk': 295, 'numerator': 2, 'denominator': 8, 'result': 0, 'score': 0})
        expected_list.append({'kpi_fk': 293, 'numerator': 155, 'denominator': 2, 'result': 0, 'score': 0})
        expected_list.append({'kpi_fk': 287, 'numerator': 1515, 'denominator': 2, 'result': 0, 'score': 0})
        kpi_results = tool_box.kpi_results
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5) if x else x)
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))