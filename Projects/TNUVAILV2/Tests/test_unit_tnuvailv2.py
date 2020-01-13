# coding=utf-8
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase, TestUnitCase
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, PropertyMock
from Projects.TNUVAILV2.Tests.data_test_unit_tnuvailv2 import DataTestUnitTnuva
from Projects.TNUVAILV2.Utils.KPIToolBox import TNUVAILToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
# import numpy as np
from pandas.util.testing import assert_frame_equal

__author__ = 'natalyak'


def get_test_case_template_all_tests():
    test_case_xls_object = pd.ExcelFile(DataTestUnitTnuva.test_case_1)
    return test_case_xls_object


def get_scif_matches_stich_groups(xls_file):
    scif_test_case = pd.read_excel(xls_file, sheetname='scif')
    matches_test_case = pd.read_excel(xls_file, sheetname='matches')
    all_products = pd.read_excel(xls_file, sheetname='all_products')
    return scif_test_case, matches_test_case, all_products


class TestTnuvailv2(TestUnitCase):

    TEST_CASE = get_test_case_template_all_tests()
    SCIF, MATCHES, ALL_PRODUCTS = get_scif_matches_stich_groups(TEST_CASE)

    @property
    def import_path(self):
        return 'Projects.TNUVAILV2.Utils.KPIToolBox'

    def set_up(self):
        super(TestTnuvailv2, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        # self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.mock_all_products()
        self.session_info_mock = self.mock_session_info()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.mock_various_project_connectors()
        self.mock_position_graph()
        self.mock_lvl3_ass_base_df()
        self.mock_own_manufacturer_property()

    # def mock_lvl3_ass_result(self):
    #     ass_res = self.mock_object('Assortment.calculate_lvl3_assortment',
    #                                path='KPIUtils_v2.Calculations.AssortmentCalculations')
    #     ass_res.return_value = DataTestUnitMarsuae.test_case_1_ass_result
    #     return ass_res.return_value

    def mock_all_products(self):
        self.data_provider_data_mock[Data.ALL_PRODUCTS] = self.ALL_PRODUCTS

    def mock_lvl3_ass_base_df(self):
        ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = DataTestUnitTnuva.assortment_store
        return ass_res.return_value

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('DBHandler.get_kpi_result_value',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        kpi_result_value.return_value = DataTestUnitTnuva.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

        def _set_scene_item_facts_for_mock(df):
            self.data_provider_data_mock[Data.SCENE_ITEM_FACTS] = df

        self.data_provider_mock._set_scene_item_facts = _set_scene_item_facts_for_mock

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2')
        self.mock_object('DbUsers', path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        self.mock_object('DbUsers', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        self.mock_object('PSProjectConnector', path='Projects.TNUVAILV2.Utils.DataBaseHandler')

    # def mock_project_connector(self):
    #     return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitTnuva.kpi_static_data
        return static_kpi.return_value

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def create_scif_matches_stitch_groups_data_mocks(self, scenes_list):
        scif_test_case = self.SCIF
        matches_test_case = self.MATCHES
        scif_scene = scif_test_case[scif_test_case['scene_fk'].isin(scenes_list)]
        matches_scene = matches_test_case[matches_test_case['scene_fk'].isin(scenes_list)]
        self.mock_scene_item_facts(scif_scene)
        self.mock_match_product_in_scene(matches_scene)
        return matches_scene, scif_scene

    def mock_get_last_session_oos_results(self, data):
        last_ses_results = self.mock_object('DBHandler.get_last_session_oos_results',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        last_ses_results.return_value = data

    def mock_get_oos_reasons_for_session(self, data):
        kpi_result_value = self.mock_object('DBHandler.get_oos_reasons_for_session',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        kpi_result_value.return_value = data

    # def mock_project_name_property(self):
    #     p = PropertyMock(return_value=self.data_provider.project_name)
    #     type(self.data_provider_mock).project_name = p

    def mock_session_info_property(self, data):
        p = PropertyMock(return_value=data)
        type(self.data_provider_mock).session_info = p

    def mock_own_manufacturer_property(self):
        p = PropertyMock(return_value=DataTestUnitTnuva.own_manuf_property)
        type(self.data_provider_mock).own_manufacturer = p

    # def test_whatever(self):
    #     matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
    #     self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
    #     self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
    #     self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
    #     tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
    #     print tool_box.kpi_result_types

    def test_get_relevant_assortment_instance_does_not_change_scif_and_data_provider_if_session_new(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        self.assertEquals(len(tool_box.scif), 13)

    def test_get_relevant_assortment_instance_changes_scif_and_data_provider_if_session_completed_and_att3_eq_scene(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 14)
        self.assertEquals(len(tool_box.scif), 14)

    def test_get_relevant_assortment_instance_does_not_change_scif_and_data_provider_if_session_completed_and_att3_not_eq_scene(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_2)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        self.assertEquals(len(tool_box.scif), 13)

    def test_get_relevant_assortment_instance_appends_rows_to_scif_and_data_provider_accordingly_based_on_oos_res(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_3)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 18)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 18)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 20)
        self.assertEquals(len(tool_box.scif), 20)
        expected_results = list()
        expected_results.append({'kpi_fk_lvl3': 3017, 'in_store': 0, 'product_fk': 2})
        expected_results.append({'kpi_fk_lvl3': 3017, 'in_store': 0, 'product_fk': 3})
        # test_result_list = []
        # for expected_result in expected_results:
        #     test_result_list.append(self.check_results(lvl3_ass_res, expected_result) == 1)
        # self.assertTrue(all(test_result_list))

    @staticmethod
    def check_results(results_df, expected_results_dict):
        column = []
        expression = []
        condition = []
        for key, value in expected_results_dict.items():
            column.append(key)
            expression.append('==')
            condition.append(value)
        query = ' & '.join('{} {} {}'.format(i, j, k) for i, j, k in zip(column, expression, condition))
        filtered_df = results_df.query(query)
        return len(filtered_df)

    # def test_calculate_availability_assortment_list_empty(self):
    #     ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
    #                                path='KPIUtils_v2.Calculations.AssortmentCalculations')
    #     ass_res.return_value = pd.DataFrame()
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([3])])
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([3])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Checkout')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3009, 'result': 0, 'score': 0, 'weight': 7.5, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_get_category_level_targets_returns_targets_for_lvl_2_kpis(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     expected_list = list()
    #     expected_list.append({'kpi_level_2_fk': 3001, 'Weight': 70})
    #     expected_list.append({'kpi_level_2_fk': 3002, 'Weight': 20})
    #     expected_list.append({'kpi_level_2_fk': 3003, 'Weight': 10})
    #     test_result_list = []
    #     for expected_result in expected_list:
    #         test_result_list.append(self.check_results(tool_box.category_params, expected_result) == 1)
    #     self.assertTrue(all(test_result_list))
    #     self.assertEquals(len(tool_box.category_params), 3)
    #
    # def test_get_kpi_category_df_matches_kpis_to_template_group(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     dict_list = tool_box.kpi_category_df.to_dict(orient='records')
    #     expected_list = list()
    #     expected_list.append({'KPI Level 2 Name': 'Chocolate & Ice Cream', 'template_group': 'Chocolate'})
    #     expected_list.append({'KPI Level 2 Name': 'Chocolate & Ice Cream', 'template_group': 'Ice Cream'})
    #     expected_list.append({'KPI Level 2 Name': 'Gum & Fruity', 'template_group': 'Gum and Confectionary'})
    #     expected_list.append({'KPI Level 2 Name': 'Pet Food', 'template_group': 'Pet food'})
    #     for expected_result in expected_list:
    #         self.assertTrue(expected_result in dict_list)
    #     self.assertEquals(len(tool_box.kpi_category_df), 4)
    #
    # def test_unpack_external_targets_json_fields_to_df_gets_all_fields_from_json_and_matches_correct_pks(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     expected_result = pd.DataFrame.from_records([
    #         {'pk': 2, u'store_att_name_1': u'store_type', u'store_att_value_1': u'SSS A'},
    #         {'pk': 3, u'store_att_name_1': u'store_type', u'store_att_value_1': u'Hypers'},
    #         {'pk': 10, u'store_att_name_1': u'store_type', u'store_att_value_1':
    #                 [u"SSS A", u"SSS B", u"Impulse A", u"Impulse B", u"Convenience A", u"Convenience B", u"Convenience C"]},
    #     ])
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_1, 'json_field')
    #     self.assertItemsEqual(['pk', 'store_att_name_1', 'store_att_value_1'], result_df.columns.values.tolist())
    #     assert_frame_equal(expected_result, result_df)
    #
    # def test_unpack_external_targets_json_fields_to_df_returns_empty_df_if_input_empty(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_empty,
    #                                                                    'json_field')
    #     self.assertTrue(result_df.empty)
    #
    # def test_unpack_external_targets_json_fields_to_df_returns_df_with_pk_field_only(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_empty_with_pks,
    #                                                                    'json_field')
    #     self.assertItemsEqual(result_df.columns.values.tolist(), ['pk'])
    #
    # def test_unpack_all_external_targets_forms_data_frame_with_all_relevant_columns_and_all_records(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     columns = tool_box.all_targets_unpacked.columns.values.tolist()
    #     expected_columns_in_output_df = DataTestUnitMarsuae.external_targets_columns
    #     validation_list = [col in columns for col in expected_columns_in_output_df]
    #     self.assertTrue(all(validation_list))
    #     self.assertEquals(len(tool_box.all_targets_unpacked), 44)
    #
    # def test_get_yes_no_result_type_fk_if_score_value_not_zero(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     expected_res = 2
    #     result_fk_distributed = tool_box.get_oos_distributed_result(1)
    #     self.assertEquals(result_fk_distributed, expected_res)
    #
    # def test_get_yes_no_result_type_fk_if_score_value_zero(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     expected_res = 1
    #     result_fk_oos = tool_box.get_oos_distributed_result(0)
    #     self.assertEquals(result_fk_oos, expected_res)
    #
    # def test_get_kpi_result_value_pk_by_value_returns_none_if_value_does_not_exist(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     result_fk = tool_box.get_kpi_result_value_pk_by_value('non_existing_value')
    #     self.assertIsNone(result_fk)
    #
    # def test_get_store_atomic_kpi_parameters_returns_atomics_relevant_to_store_in_session(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     expected_result = [55, 56, 57, 58, 59, 60, 61, 62, 63, 71, 72, 73, 74, 75, 76, 77, 78, 81, 82]
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     self.assertItemsEqual(store_atomics['pk'].values.tolist(), expected_result)
    #
    # def test_get_store_atomic_kpi_parameters_returns_empty_df_if_no_relevant_atomics_for_policy(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.store_info_dict = DataTestUnitMarsuae.store_info_dict_other_type
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     self.assertTrue(store_atomics.empty)
    #
    # def test_calculate_atomics_does_not_calculate_atomics_if_no_relevant_atomics_for_policy(self):
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2])])
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.store_info_dict = DataTestUnitMarsuae.store_info_dict_other_type
    #     tool_box.calculate_atomics()
    #     self.assertTrue(tool_box.atomic_kpi_results.empty)
    #
    # def test_get_atomics_for_template_groups_present_in_store_returns_both_choc_and_ice_cream_atomic_kpis(self):
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2])])
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.get_atomics_for_template_groups_present_in_store(store_atomics)
    #     expected_result = range(55, 64)
    #     self.assertItemsEqual(store_atomics['pk'].values.tolist(), expected_result)
    #     self.assertItemsEqual(store_atomics['KPI Level 2 Name'].unique().tolist(), ['Chocolate & Ice Cream'])
    #
    # def test_get_atomics_for_template_groups_present_in_store_returns_atomic_kpis_if_scene_in_scene_info(self):
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([7, 13])])
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([7, 13])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.get_atomics_for_template_groups_present_in_store(store_atomics)
    #     expected_result = range(55, 64) + range(71, 79)
    #     self.assertItemsEqual(store_atomics['pk'].values.tolist(), expected_result)
    #     self.assertItemsEqual(store_atomics['KPI Level 2 Name'].unique().tolist(), ['Chocolate & Ice Cream',
    #                                                                                 'Gum & Fruity'])
    #
    # def test_main_calculation_calculates_2_category_lvl_even_if_one_scene_has_no_scif(self):
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([7, 13])])
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([7, 13])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.main_calculation()
    #     self.assertItemsEqual(tool_box.cat_lvl_res['kpi_type'].unique().tolist(), ['Chocolate & Ice Cream',
    #                                                                                'Gum & Fruity'])
    #
    # def test_main_calculation_scif_empty_one_scene_without_tags(self):
    #     self.mock_scene_item_facts(pd.DataFrame(columns=['pk', 'session_id', 'store_id', 'visit_date', 'scene_id',
    #                                                      'item_id', 'template_fk', 'template_name', 'facings',
    #                                                      'product_fk', 'scene_fk', 'gross_len_ign_stack',
    #                                                      'category_fk', 'manufacturer_fk', 'sub_category_fk',
    #                                                      'brand_fk']))
    #     self.mock_match_product_in_scene(pd.DataFrame(columns=['scene_match_fk', 'scene_fk', 'product_fk',
    #                                                            'probe_match_fk', 'stacking_layer']))
    #     probe_group = self.mock_probe_group(pd.DataFrame(columns=['probe_group_id', 'probe_match_fk']))
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([13])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.main_calculation()
    #     self.assertEquals(tool_box.total_score, 5)
    #     self.assertEquals(tool_box.cat_lvl_res['kpi_type'].values.tolist(), ['Chocolate & Ice Cream'])
    #     self.assertItemsEqual(tool_box.atomic_kpi_results.kpi_fk.values.tolist(), [3011, 3014, 3030, 3029, 3005,
    #                                                                                3009, 3010, 3028, 3025])
    #
    # def test_build_tiers_for_atomics(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     expected_kpi_list = ['NBL - Chocolate Main', 'NBL - Chocolate Checkout', 'NBL - Gum Main', 'NBL - Gum Checkout',
    #                          'NBL - Petfood Main', 'NBL - Ice Cream Main']
    #     self.assertItemsEqual(tool_box.atomic_tiers_df['kpi_type'].unique().tolist(), expected_kpi_list)
    #     expected_list = list()
    #     expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0, 'step_value': 0.6})
    #     expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0.5, 'step_value': 0.8})
    #     expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0.8, 'step_value': 0.96})
    #     expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 1, 'step_value': 1.01})
    #
    #     expected_list.append({'kpi_type': 'NBL - Gum Checkout', 'step_score_value': 0, 'step_value': 0.93})
    #     expected_list.append({'kpi_type': 'NBL - Gum Checkout', 'step_score_value': 1, 'step_value': 1.01})
    #     test_result_list = tool_box.atomic_tiers_df.to_dict(orient='records')
    #     for expected_result in expected_list:
    #         self.assertTrue(expected_result in test_result_list)
    #
    # def test_calculate_checkouts_less_then_target(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 12])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_checkout_1)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.common.write_to_db_result = MagicMock()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Checkout Penetration - Chocolate')
    #     tool_box.calculate_checkouts(param_row)
    #     expected_result = {'kpi_fk': 3005, 'result': 50, 'score': 0, 'weight': 7.5, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    #     duplicate_parent_res = tool_box.common.write_to_db_result.mock_calls[0][2]
    #     duplicate_res = tool_box.common.write_to_db_result.mock_calls[1][2]
    #     self.assertEquals(duplicate_res['numerator_result'], 1)
    #     self.assertEquals(duplicate_res['denominator_result'], 2)
    #     self.assertEquals(duplicate_res['fk'], 3035)
    #     self.check_duplicate_kpi_results_mirrors_parent(duplicate_parent_res, duplicate_res)
    #
    # def test_calculate_checkouts_more_than_target(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 12])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_checkout_2)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.common.write_to_db_result = MagicMock()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Checkout Penetration - Chocolate')
    #     tool_box.calculate_checkouts(param_row)
    #     atomic_res = tool_box.atomic_kpi_results
    #     atomic_res['result'] = atomic_res['result'].apply(lambda x: round(x, 5))
    #     expected_result = {'kpi_fk': 3005, 'result': round(2/3.0 * 100, 5), 'score': 1, 'weight': 7.5, 'score_by_weight': 7.5}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    #     duplicate_parent_res = tool_box.common.write_to_db_result.mock_calls[0][2]
    #     duplicate_res = tool_box.common.write_to_db_result.mock_calls[1][2]
    #     self.assertEquals(duplicate_res['numerator_result'], 2)
    #     self.assertEquals(duplicate_res['denominator_result'], 3)
    #     self.assertEquals(duplicate_res['fk'], 3035)
    #     self.check_duplicate_kpi_results_mirrors_parent(duplicate_parent_res, duplicate_res)
    #
    # def test_calculate_checkouts_one_scene_wo_tags(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([2, 3, 14])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_checkout_count_including_no_tags)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.common.write_to_db_result = MagicMock()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Checkout Penetration - Chocolate')
    #     tool_box.calculate_checkouts(param_row)
    #     atomic_res = tool_box.atomic_kpi_results
    #     atomic_res['result'] = atomic_res['result'].apply(lambda x: round(x, 5))
    #     expected_result = {'kpi_fk': 3005, 'result': round(2/3.0 * 100, 5), 'score': 1, 'weight': 7.5, 'score_by_weight': 7.5}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    #     duplicate_parent_res = tool_box.common.write_to_db_result.mock_calls[0][2]
    #     duplicate_res = tool_box.common.write_to_db_result.mock_calls[1][2]
    #     self.assertEquals(duplicate_res['numerator_result'], 2)
    #     self.assertEquals(duplicate_res['denominator_result'], 3)
    #     self.assertEquals(duplicate_res['fk'], 3035)
    #     self.check_duplicate_kpi_results_mirrors_parent(duplicate_parent_res, duplicate_res)
    #
    # def test_calculate_availability_no_products_from_list_in_session(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([3])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([3])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Checkout')
    #     tool_box.calculate_availability(param_row)
    #     expected_result = {'kpi_fk': 3009, 'result': 0, 'score': 0, 'weight': 7.5, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_availability_sku_lvl_no_products_in_list(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([3])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([3])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Checkout')
    #     lvl3_ass_res = tool_box.lvl3_assortment[tool_box.lvl3_assortment['ass_lvl2_kpi_type']
    #                                             == param_row[tool_box.KPI_TYPE]]
    #     lvl3_ass_res = tool_box.calculate_lvl_3_assortment_result(lvl3_ass_res, param_row)
    #     expected_results = list()
    #     expected_results.append({'kpi_fk_lvl3': 3017, 'in_store': 0, 'product_fk': 2})
    #     expected_results.append({'kpi_fk_lvl3': 3017, 'in_store': 0, 'product_fk': 3})
    #     test_result_list = []
    #     for expected_result in expected_results:
    #         test_result_list.append(self.check_results(lvl3_ass_res, expected_result) == 1)
    #     self.assertTrue(all(test_result_list))
    #
    # def test_calculate_availability_products_from_list_exist_in_session(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Checkout')
    #     tool_box.calculate_availability(param_row)
    #     expected_result = {'kpi_fk': 3009, 'result': 0.5, 'score': 0, 'weight': 7.5, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_availability_products_from_list_exist_in_session_tiers(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Main')
    #     tool_box.calculate_availability(param_row)
    #     atomic_results = tool_box.atomic_kpi_results
    #     atomic_results['result'] = atomic_results['result'].apply(lambda x: round(x, 5))
    #     expected_result = {'kpi_fk': 3011, 'result': round(2/3.0, 5), 'score': 0.5, 'weight': 10, 'score_by_weight': 5}
    #     check = self.check_results(atomic_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_availability_sku_lvl_products_in_list_in_session(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'NBL - Chocolate Main')
    #     lvl3_ass_res = tool_box.lvl3_assortment[tool_box.lvl3_assortment['ass_lvl2_kpi_type']
    #                                             == param_row[tool_box.KPI_TYPE]]
    #     lvl3_ass_res = tool_box.calculate_lvl_3_assortment_result(lvl3_ass_res, param_row)
    #     expected_results = list()
    #     expected_results.append({'kpi_fk_lvl3': 3019, 'in_store': 1, 'product_fk': 2})
    #     expected_results.append({'kpi_fk_lvl3': 3019, 'in_store': 1, 'product_fk': 1})
    #     expected_results.append({'kpi_fk_lvl3': 3019, 'in_store': 0, 'product_fk': 12})
    #     test_result_list = []
    #     for expected_result in expected_results:
    #         test_result_list.append(self.check_results(lvl3_ass_res, expected_result) == 1)
    #     self.assertTrue(all(test_result_list))
    #
    # def test_calculate_display_number_if_displays_equals_target(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_display_1)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'POI Compliance - Chocolate / Ice Cream')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3025, 'result': 1, 'score': 1, 'weight': 5, 'score_by_weight': 5}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_display_number_if_no_relevant_displays_in_session(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_display_2)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'POI Compliance - Chocolate / Ice Cream')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3025, 'result': 0, 'score': 0, 'weight': 5, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_display_display_number_more_than_target(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4, 5])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_display_3)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'POI Compliance - Chocolate / Ice Cream')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3025, 'result': 2, 'score': 2, 'weight': 5, 'score_by_weight': 10}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_display_number_if_displays_equals_target_no_tags_in_scif(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 13])
    #     self.mock_all_scenes_in_session(DataTestUnitMarsuae.scenes_for_display_including_no_tags)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'POI Compliance - Chocolate / Ice Cream')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3025, 'result': 1, 'score': 1, 'weight': 5, 'score_by_weight': 5}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_linear_sos(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4, 5, 6])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3, 4, 5, 6])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.common.write_to_db_result = MagicMock()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'SOS - Gum/Fruity Checkout')
    #     tool_box.calculate_atomic_results(param_row)
    #     kpi_result = tool_box.atomic_kpi_results
    #     kpi_result['result'] = kpi_result['result'].apply(lambda x: round(x, 5))
    #     kpi_result['score'] = kpi_result['score'].apply(lambda x: round(x, 5))
    #     expected_result = {'kpi_fk': 3033, 'result': round((46/52.0)*100, 5), 'score': round((46/52.0)/0.77, 5),
    #                        'weight': 0, 'score_by_weight': 0}
    #     check = self.check_results(kpi_result, expected_result)
    #     self.assertEquals(check, 1)
    #
    #     duplicate_parent_res = tool_box.common.write_to_db_result.mock_calls[0][2]
    #     duplicate_res = tool_box.common.write_to_db_result.mock_calls[1][2]
    #     self.assertEquals(duplicate_res['numerator_result'], 46)
    #     self.assertEquals(duplicate_res['denominator_result'], 52)
    #     self.assertEquals(duplicate_res['fk'], 3043)
    #     self.check_duplicate_kpi_results_mirrors_parent(duplicate_parent_res, duplicate_res)
    #
    # def test_calculate_kpi_combination_score_two_child_kpis_pass(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4, 5, 6])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3, 4, 5, 6])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_kpi_combination_test_1
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'Gum / Fruity Checkout Compliance')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3008, 'result': 100, 'score': 1, 'weight': 40, 'score_by_weight': 40}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_kpi_combination_score_one_child_kpi_passes(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4, 5, 6])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3, 4, 5, 6])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_kpi_combination_test_2
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'Gum / Fruity Checkout Compliance')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3008, 'result': 100, 'score': 1, 'weight': 40, 'score_by_weight': 40}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_kpi_combination_score_none_child_kpi_passes(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2, 3, 4, 5, 6])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 2, 3, 4, 5, 6])])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_kpi_combination_test_3
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics,
    #                                                               'Gum / Fruity Checkout Compliance')
    #     tool_box.calculate_atomic_results(param_row)
    #     expected_result = {'kpi_fk': 3008, 'result': 0, 'score': 0, 'weight': 40, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_category_level_categories(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_cat_level_all_cat
    #     tool_box.calculate_category_level()
    #     expected_results = list()
    #     expected_results.append({'kpi_type': 'Chocolate & Ice Cream', 'cat_score': 45})
    #     expected_results.append({'kpi_type': 'Gum & Fruity', 'cat_score': 10})
    #     expected_results.append({'kpi_type': 'Pet Food', 'cat_score': 100})
    #     cat_lvl_res = tool_box.cat_lvl_res[['kpi_type', 'cat_score']]
    #     cat_lvl_dict = cat_lvl_res.to_dict(orient='records')
    #     for expected_result in expected_results:
    #         self.assertTrue(expected_result in cat_lvl_dict)
    #     self.assertEquals(len(tool_box.cat_lvl_res), 3)
    #
    # def test_calculate_category_level_no_atomic_results(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', 'kpi_type', 'result', 'score', 'weight',
    #                                                         'score_by_weight', 'parent_name'])
    #     tool_box.calculate_category_level()
    #     self.assertTrue(tool_box.cat_lvl_res.empty)
    #
    # def test_calculate_total_score_3_categories(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_cat_level_all_cat
    #     tool_box.calculate_category_level()
    #     tool_box.calculate_total_score()
    #     self.assertEquals(tool_box.total_score, 43.5)
    #
    # def test_calculate_category_level_2_categories(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.atomic_kpi_results = DataTestUnitMarsuae.kpi_results_df_for_cat_level_2_cat
    #     tool_box.calculate_category_level()
    #     tool_box.calculate_total_score()
    #     self.assertEquals(tool_box.total_score, 38.75)
    #
    # def test_calculate_category_level_empty(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.atomic_kpi_results = pd.DataFrame(columns=['kpi_fk', 'kpi_type', 'result', 'score', 'weight',
    #                                                         'score_by_weight', 'parent_name'])
    #     tool_box.calculate_category_level()
    #     tool_box.calculate_total_score()
    #     self.assertEquals(tool_box.total_score, 0)
    #
    # def test_get_tiered_score_gets_relevant_score_if_result_in_lower_range(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = pd.Series({'score_logic': 'Tiered', 'Weight': 10, 'kpi_type': 'NBL - Chocolate Main'})
    #     score, weight = tool_box.get_score(0.5, param_row)
    #     self.assertEquals(score, 0)
    #     self.assertEquals(weight, 10)
    #
    # def test_get_tiered_score_gets_relevant_score_if_result_at_border_value(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = pd.Series({'score_logic': 'Tiered', 'Weight': 10, 'kpi_type': 'NBL - Chocolate Main'})
    #     score, weight = tool_box.get_score(0.8, param_row)
    #     self.assertEquals(score, 0.8)
    #     self.assertEquals(weight, 10)
    #
    # def test_get_tiered_score_gets_score_zero_if_result_is_zero(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = pd.Series({'score_logic': 'Tiered', 'Weight': 10, 'kpi_type': 'NBL - Chocolate Main'})
    #     score, weight = tool_box.get_score(0, param_row)
    #     self.assertEquals(score, 0)
    #     self.assertEquals(weight, 10)
    #
    # def test_get_tiered_score_gets_relevant_score_if_result_in_upper_range(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = pd.Series({'score_logic': 'Tiered', 'Weight': 10, 'kpi_type': 'NBL - Gum Main'})
    #     score, weight = tool_box.get_score(1, param_row)
    #     self.assertEquals(score, 1)
    #     self.assertEquals(weight, 10)
    #
    # def test_get_score_returns_zero_if_score_logic_is_not_defined(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Not_existing_logic', 'Weight': 10, 'kpi_type': 'kpi1'})
    #     score, weight = tool_box.get_score(1, param_row)
    #     self.assertEquals(score, 0)
    #     self.assertEquals(weight, 10)
    #
    # def test_binary_score_result_exceeds_target(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Binary', 'Weight': 50, 'Target': 2, 'kpi_type': 'kpi1'})
    #     score, weight = tool_box.get_score(result=3, param_row=param_row)
    #     self.assertEquals(score, 1)
    #     self.assertEquals(weight, 50)
    #
    # def test_binary_score_result_equals_target(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Binary', 'Weight': 10, 'Target': 0.5, 'kpi_type': 'kpi1'})
    #     score, weight = tool_box.get_score(result=0.5, param_row=param_row)
    #     self.assertEquals(score, 1)
    #     self.assertEquals(weight, 10)
    #
    # def test_binary_score_result_less_than_target(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Binary', 'Weight': 10, 'Target': 0.5, 'kpi_type': 'kpi1'})
    #     score, weight = tool_box.get_score(result=0.2, param_row=param_row)
    #     self.assertEquals(score, 0)
    #     self.assertEquals(weight, 10)
    #
    # def test_get_relative_score(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Relative Score', 'Weight': 20, 'Target': 2})
    #     score, weight = tool_box.get_score(result=1, param_row=param_row)
    #     self.assertEquals(score, 0.5)
    #     self.assertEquals(weight, 20)
    #
    # def test_get_relative_score_if_target_zero(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     param_row = pd.Series({'score_logic': 'Relative Score', 'Weight': 20, 'Target': 0})
    #     score, weight = tool_box.get_score(result=1, param_row=param_row)
    #     self.assertEquals(score, 0)
    #     self.assertEquals(weight, 20)
    #
    # def test_process_targets_appropriately_processes_the_target_field_of_external_targets(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     input_df = DataTestUnitMarsuae.input_df_for_targets
    #     input_df['Target'] = input_df.apply(tool_box.process_targets, axis=1)
    #     input_df_dict = input_df.to_dict(orient='records')
    #     expected_list = list()
    #     expected_list.append({'score_logic': 'Binary', 'Target': 0.5, 'kpi_type': 'kpi_a', 'kpi_fk': 1})
    #     expected_list.append({'score_logic': 'Relative Score', 'Target': 0, 'kpi_type': 'kpi_b', 'kpi_fk': 2})
    #     expected_list.append({'score_logic': 'Binary', 'Target': 0, 'kpi_type': 'kpi_c', 'kpi_fk': 3})
    #     expected_list.append({'score_logic': 'Tiered', 'Target': '', 'kpi_type': 'kpi_d', 'kpi_fk': 4})
    #     expected_list.append({'score_logic': 'Relative Score', 'Target': 0, 'kpi_type': 'kpi_e', 'kpi_fk': 5})
    #     for expected_result in expected_list:
    #         self.assertTrue(expected_result in input_df_dict)
    #
    # def test_calculate_block_does_not_write_results_if_ass_empty(self):
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.lvl3_assortment = pd.DataFrame()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_atomic_results(param_row)
    #     self.assertTrue(tool_box.atomic_kpi_results.empty)
    #
    # def test_calculate_block_results_empty(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 9])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 9])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_empty])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0, 'score': 0, 'weight': 10, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_all_blocks_fail(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 9])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 9])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_failed])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0, 'score': 0, 'weight': 10, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_one_block_passes_one_scene_2_sku_types(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 7])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 8])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_sc_7])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0.4, 'score': 1, 'weight': 10, 'score_by_weight': 10}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_one_block_passes_one_scene_one_sku_type(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 8])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 8])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_sc_8])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0.2, 'score': 0, 'weight': 10, 'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_two_scenes_returns_result_for_largest_number_of_skus(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 7, 8])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 7, 8])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_sc_7, DataTestUnitMarsuae.block_results_sc_8])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0.4, 'score': 1, 'weight': 10, 'score_by_weight': 10}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_two_blocks_pass_in_one_scene_returns_share_for_max_sku_types(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 10])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 10])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_sc_10])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0.6, 'score': 1, 'weight': 10, 'score_by_weight': 10}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_calculate_block_3_scenes_max_block(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 7, 8, 10])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([1, 7, 8, 10])])
    #     self.mock_block_results([DataTestUnitMarsuae.block_results_sc_7, DataTestUnitMarsuae.block_results_sc_8,
    #                              DataTestUnitMarsuae.block_results_sc_10])
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'Red Block Compliance - Main')
    #     tool_box.calculate_block(param_row)
    #     expected_result = {'kpi_fk': 3016, 'result': 0.6, 'score': 1, 'weight': 10, 'score_by_weight': 10}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    # def test_sos_gum_with_additional_exclusion_parameter(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks([11])
    #     self.mock_all_scenes_in_session(
    #         DataTestUnitMarsuae.scenes_full_df[DataTestUnitMarsuae.scenes_full_df['scene_fk'].isin([11])])
    #     self.mock_store_data_test_case(DataTestUnitMarsuae.store_data_supers_a)
    #     tool_box = MARSUAEToolBox(self.data_provider_mock, self.output)
    #     tool_box.common.write_to_db_result = MagicMock()
    #     store_atomics = tool_box.get_store_atomic_kpi_parameters()
    #     tool_box.build_tiers_for_atomics(store_atomics)
    #     store_atomics = tool_box.add_duplicate_kpi_fk_where_applicable(store_atomics)
    #     param_row = self.get_parameter_series_for_kpi_calculation(store_atomics, 'SOS - Gum Main')
    #     tool_box.calculate_atomic_results(param_row)
    #     kpi_result = tool_box.atomic_kpi_results
    #     kpi_result['result'] = kpi_result['result'].apply(lambda x: round(x, 5))
    #     expected_result = {'kpi_fk': 3032, 'result': round(2/22.0*100, 5), 'score': 0, 'weight': 15,
    #                        'score_by_weight': 0}
    #     check = self.check_results(tool_box.atomic_kpi_results, expected_result)
    #     self.assertEquals(check, 1)
    #
    #     duplicate_parent_res = tool_box.common.write_to_db_result.mock_calls[0][2]
    #     duplicate_res = tool_box.common.write_to_db_result.mock_calls[1][2]
    #     self.assertEquals(duplicate_res['numerator_result'], 2)
    #     self.assertEquals(duplicate_res['denominator_result'], 22)
    #     self.assertEquals(duplicate_res['fk'], 3042)
    #     self.check_duplicate_kpi_results_mirrors_parent(duplicate_parent_res, duplicate_res)
    #


    # @staticmethod
    # def get_parameter_series_for_kpi_calculation(store_atomics, kpi_name):
    #     param_df = store_atomics[store_atomics['kpi_type'] == kpi_name]
    #     param_index = param_df.index.values[0]
    #     param_row = store_atomics.iloc[param_index]
    #     return param_row
    #
    # def check_duplicate_kpi_results_mirrors_parent(self, duplicate_parent_res, duplicate_res):
    #     self.assertEquals(duplicate_parent_res['numerator_result'], duplicate_res['numerator_result'])
    #     self.assertEquals(duplicate_parent_res['denominator_result'], duplicate_res['denominator_result'])
    #     self.assertEquals(duplicate_parent_res['score'], duplicate_res['score'])
    #     self.assertEquals(duplicate_parent_res['result'], duplicate_res['result'])
    #     self.assertEquals(duplicate_parent_res['target'], duplicate_res['target'])
    #     self.assertEquals(duplicate_res['identifier_parent']['kpi_fk'], duplicate_parent_res['fk'])
    #     self.assertEquals(duplicate_res['identifier_parent'], duplicate_parent_res['identifier_result'])