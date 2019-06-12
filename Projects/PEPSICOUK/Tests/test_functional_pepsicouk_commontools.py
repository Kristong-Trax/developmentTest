from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from mock import MagicMock
from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKCommonToolBox
from Projects.PEPSICOUK.Tests.data_test_unit_pepsicouk import DataTestUnitPEPSICOUK
from pandas.util.testing import assert_frame_equal
import os
import pandas as pd
import numpy as np

__author__ = 'natalya'


class Test_PEPSICOUKCommon(TestFunctionalCase):
    # seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.Utils.CommonToolBox'

    def set_up(self):
        super(Test_PEPSICOUKCommon, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.db_users_mock = self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        self.full_store_data_mock = self.mock_store_data()

        self.custom_entity_data_mock = self.mock_custom_entity_data()
        self.on_display_products_mock = self.mock_on_display_products()

        self.exclusion_template_mock = self.mock_template_data()
        self.store_policy_template_mock = self.mock_store_policy_exclusion_template_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        self.mock_all_products()
        self.mock_all_templates()

    def mock_db_users(self):
        return self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2')

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
                                                                     sheetname='all_products')

    def mock_all_templates(self):
        self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    def mock_kpi_external_targets_data(self):
        print DataTestUnitPEPSICOUK.external_targets
        external_targets_df = pd.read_excel(DataTestUnitPEPSICOUK.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_on_display_products(self):
        on_display_products = self.mock_object('PEPSICOUKCommonToolBox.get_on_display_products')
        on_display_products.return_value = DataTestUnitPEPSICOUK.on_display_products
        return on_display_products.return_value

    def mock_custom_entity_data(self):
        custom_entities = self.mock_object('PEPSICOUKCommonToolBox.get_custom_entity_data')
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
        # return static_kpi

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKCommonToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_store_policy_exclusion_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path, sheet_name='store_policy')
        template_df = template_df.fillna('ALL')
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_store_policy_data_for_exclusion_template',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value

    def mock_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path, sheet_name='exclusion_rules')
        template_df = template_df.fillna('')
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value
        # return template_data_mock

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox',
                                           )
        kpi_result_value.return_value = DataTestUnitPEPSICOUK.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_kpi_score_value_table(self):
        kpi_score_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_score_values_df',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox',)
        kpi_score_value.return_value = DataTestUnitPEPSICOUK.kpi_scores_values_table
        return kpi_score_value.return_value

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_all_products_in_data_provider(self, data):
        self.data_provider_data_mock['all_products'] = data.where(data.notnull(), None)

    def test_unpack_external_targets_json_fields_to_df_gets_all_fields_from_json_and_matches_correct_pks(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_result = pd.DataFrame.from_records([
            {'pk': 2, u'store_type': u'CORE', u'additional_attribute_2': np.nan, u'additional_attribute_1': u'ALL'},
            {'pk': 3, u'store_type': u'ALL', u'additional_attribute_2': np.nan, u'additional_attribute_1': u'OT'},
            {'pk': 10, u'store_type': u'CORE', u'additional_attribute_2': u'SAINSBURY', u'additional_attribute_1': np.nan},
        ])
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_1, 'json_field')
        self.assertItemsEqual(['pk', 'store_type', 'additional_attribute_1','additional_attribute_2'],
                              result_df.columns.values.tolist())
        assert_frame_equal(expected_result, result_df)

    def test_unpack_external_targets_json_fields_to_df_returns_empty_df_if_input_empty(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_empty,
                                                                       'json_field')
        self.assertTrue(result_df.empty)

    def test_unpack_external_targets_json_fields_to_df_returns_df_with_pk_field_only(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_empty_with_pks,
                                                                       'json_field')
        self.assertItemsEqual(result_df.columns.values.tolist(), ['pk'])

    def test_excluded_matches_are_not_present_in_filtered_matches_and_included_are_present(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        excluded_matches = tool_box.match_product_in_scene[~(tool_box.match_product_in_scene['Out'].
                                        isnull())]['probe_match_fk'].values.tolist()
        matches_negative_check = tool_box.filtered_matches[tool_box.filtered_matches['probe_match_fk'].\
                                                isin(excluded_matches)]
        self.assertTrue(matches_negative_check.empty)
        included_matches = tool_box.match_product_in_scene[tool_box.match_product_in_scene['Out'].
                                        isnull()]['probe_match_fk'].values.tolist()
        matches_positive_check = tool_box.filtered_matches[tool_box.filtered_matches['probe_match_fk'].\
                                                isin(included_matches)]
        self.assertItemsEqual(included_matches, matches_positive_check['probe_match_fk'].values.tolist())

    def test_filtered_scif_facings_and_gross_length_reduced_following_matches(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        for i, row in tool_box.filtered_scif.iterrows():
            expected_facings = len(tool_box.filtered_matches[(tool_box.filtered_matches['scene_fk'] == row['scene_fk']) &
                                                             (tool_box.filtered_matches['product_fk'] == row['product_fk'])])
            expected_len = tool_box.filtered_matches[(tool_box.filtered_matches['scene_fk'] == row['scene_fk']) &
                                                     (tool_box.filtered_matches['product_fk'] == row['product_fk'])] \
                                                    ['width_mm_advance'].sum()
            self.assertEquals(row['facings'], expected_facings)
            self.assertEquals(row['gross_len_add_stack'], expected_len)

    def test_filters_for_scif_and_matches_are_retrieved_in_the_right_format(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_result = {'smart_attribute_state': (['additional display'], 0),
                           'location_type': ['Primary Shelf'],
                           'product_name': (['General Empty'], 0)}
        excl_template_all_kpis = tool_box.exclusion_template[tool_box.exclusion_template['KPI'].str.upper() == 'ALL']
        template_filters = tool_box.get_filters_dictionary(excl_template_all_kpis)
        self.assertDictEqual(expected_result, template_filters)

    def test_filters_for_scif_and_matches_return_empty_dict_if_template_empty(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_result = {}
        template_filters = tool_box.get_filters_dictionary(DataTestUnitPEPSICOUK.empty_exclusion_template)
        self.assertDictEqual(expected_result, template_filters)
        self.assertIsInstance(template_filters, dict)

    def test_filters_for_scif_and_matches_return_empty_dict_with_ommitted_action_field(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_result = {'category': (['Cat 1', 'Cat 2'], 0),
                           'location_type': ['Primary Shelf']}
        template_filters = tool_box.get_filters_dictionary(DataTestUnitPEPSICOUK.exclusion_template_missing_action)
        self.assertDictEqual(expected_result, template_filters)

    def test_get_filters_for_scif_and_matches_returns_values_with_product_and_scene_pks(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        excl_template_all_kpis = tool_box.exclusion_template[tool_box.exclusion_template['KPI'].str.upper() == 'ALL']
        template_filters = tool_box.get_filters_dictionary(excl_template_all_kpis)
        filters = tool_box.get_filters_for_scif_and_matches(template_filters)
        expected_result = {'scene_fk': [1, 2], 'product_fk': [1, 2, 3, 4, 5]}
        self.assertDictEqual(filters, expected_result)

    def test_unpack_all_external_targets_forms_data_frame_with_all_relevant_columns_and_all_records(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        columns = tool_box.all_targets_unpacked.columns.values.tolist()
        expected_columns_in_output_df = DataTestUnitPEPSICOUK.external_targets_columns
        validation_list = [col in columns for col in expected_columns_in_output_df]
        self.assertTrue(all(validation_list))
        self.assertEquals(len(tool_box.all_targets_unpacked), 32)

    def test_unpack_all_external_targets_kpi_relevant_columns_are_filled(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        targets = tool_box.all_targets_unpacked
        shelf_placement_targets = targets[targets['operation_type'] == tool_box.SHELF_PLACEMENT]
        data_with_null_values = shelf_placement_targets[shelf_placement_targets \
                                            ['Shelves From Bottom To Include (data)'].isnull()]
        self.assertEquals(len(data_with_null_values), 0)
        keys_with_null_values = shelf_placement_targets[shelf_placement_targets \
                    ['No of Shelves in Fixture (per bay) (key)'].isnull()]
        self.assertEquals(len(keys_with_null_values), 0)
        # sos_vs_index_targets = targets[targets['operation_type'] == tool_box.SOS_VS_TARGET]

    def test_get_yes_no_result_type_fk_if_score_value_not_zero(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_res = 4
        result_fk = tool_box.get_yes_no_result(1)
        self.assertEquals(result_fk, expected_res)

    def test_get_yes_no_result_type_fk_if_score_value_zero(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_res = 5
        result_fk = tool_box.get_yes_no_result(0)
        self.assertEquals(result_fk, expected_res)

    def test_get_yes_no_score_type_fk_if_score_value_not_zero(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        expected_res = 4
        result_fk = tool_box.get_yes_no_score(1)
        self.assertEquals(result_fk, expected_res)

    def test_get_yes_no_score_type_fk_if_score_value_zero(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        print tool_box.kpi_score_values
        expected_res = 5
        result_fk = tool_box.get_yes_no_score(0)
        self.assertEquals(result_fk, expected_res)

    def test_get_kpi_result_value_pk_by_value_returns_none_if_value_does_not_exist(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result_fk = tool_box.get_kpi_result_value_pk_by_value('non_existing_value')
        self.assertIsNone(result_fk)

    def test_get_kpi_score_value_pk_by_value_returns_none_if_value_does_not_exist(self):
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result_fk = tool_box.get_kpi_score_value_pk_by_value('non_existing_value')
        self.assertIsNone(result_fk)

    def test_empty_scene_details(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_2, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_2, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        self.assertTrue(tool_box.scif.empty)
        self.assertTrue(tool_box.match_product_in_scene.empty)
        self.assertTrue(tool_box.filtered_matches.empty)
        self.assertTrue(tool_box.filtered_scif.empty)

    def test_do_exclusion_rules_apply_to_store_returns_true_if_kpi_not_in_store_policy_tab(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result = tool_box.do_exclusion_rules_apply_to_store('ALL')
        self.assertTrue(result)

    def test_do_exclusion_rules_apply_to_store_returns_true_if_kpi_in_store_policy_tab_and_store_attributes_comply(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result = tool_box.do_exclusion_rules_apply_to_store('Sub Brand Space to Sales Index')
        self.assertTrue(result)

    def test_do_exclusion_rules_apply_to_store_returns_false_if_kpi_in_store_policy_tab_and_store_attr_do_not_match_policy(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result = tool_box.do_exclusion_rules_apply_to_store('PepsiCo Segment Space to Sales Index')
        self.assertFalse(result)

    def test_do_exclusion_rules_apply_to_store_returns_true_if_kpi_in_store_policy_tab_and_store_attributes_comply_and_values_with_comma(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        result = tool_box.do_exclusion_rules_apply_to_store('PepsiCo Sub Segment Space to Sales Index')
        self.assertTrue(result)

    def test_set_filtered_scif_and_matches_for_specific_kpi_additionally_filter_scif_and_matches(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        scif, matches = tool_box.set_filtered_scif_and_matches_for_specific_kpi(tool_box.filtered_scif, tool_box.filtered_matches,
                                                                                'Brand Space to Sales Index')
        matches_excluded_kpi_specific = set(tool_box.filtered_matches['probe_match_fk'].values.tolist()) - \
                                            set(matches['probe_match_fk'].values.tolist())
        self.assertEquals(len(matches), 30)
        self.assertNotEqual(len(matches), len(tool_box.filtered_matches))
        included_matches_expected = [3, 4, 5, 6, 7, 8, 14, 15, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43]
        excluded_matches_expected = [11, 12, 13, 20, 29, 30, 31, 32, 44]
        self.assertItemsEqual(matches_excluded_kpi_specific, excluded_matches_expected)
        matches_positive_check = tool_box.filtered_matches[tool_box.filtered_matches['probe_match_fk']. \
            isin(included_matches_expected)]
        self.assertItemsEqual(included_matches_expected, matches_positive_check['probe_match_fk'].values.tolist())
        self.assertEquals(len(scif), 5)
        self.assertNotEqual(len(scif), len(tool_box.filtered_scif))

    def test_set_filtered_scif_and_matches_for_specific_kpi_does_not_change_filtered_scif_and_matches_if_no_policy_applies(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
        scif, matches = tool_box.set_filtered_scif_and_matches_for_specific_kpi(tool_box.filtered_scif,
                                                                                tool_box.filtered_matches,
                                                                                'PepsiCo Segment Space to Sales Index')
        self.assertEquals(len(matches), len(tool_box.filtered_matches))
        self.assertEquals(len(scif), len(tool_box.filtered_scif))
        assert_frame_equal(scif, tool_box.filtered_scif)
        assert_frame_equal(matches, tool_box.filtered_matches)

    # def test_whatever(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    #     print tool_box.exclusion_template


