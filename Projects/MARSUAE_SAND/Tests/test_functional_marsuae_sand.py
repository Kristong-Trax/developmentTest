from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.MARSUAE_SAND.Tests.data_test_unit_marsuae_sand import DataTestUnitMarsuae
from Projects.MARSUAE_SAND.Utils.KPIToolBox import MARSUAE_SANDToolBox
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal

__author__ = 'natalyak'


class TestMarsuaeSand(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Projects.MARSUAE_SAND.Utils.KPIToolBox'

    def set_up(self):
        super(TestMarsuaeSand, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        # self.probe_group_mock = self.mock_probe_group()
        self.full_store_info = self.mock_store_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.mock_all_products()
        self.mock_all_templates()
        self.mock_block()
        self.mock_various_project_connectors()
        self.mock_position_graph()
        self.mock_lvl3_ass_base_df()

    # def mock_lvl3_ass_result(self):
    #     ass_res = self.mock_object('Assortment.calculate_lvl3_assortment',
    #                                path='KPIUtils_v2.Calculations.AssortmentCalculations')
    #     ass_res.return_value = DataTestUnitMarsuae.test_case_1_ass_result
    #     return ass_res.return_value

#start here
    def mock_lvl3_ass_base_df(self):
        ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = DataTestUnitMarsuae.assortment_store_sss_a
        return ass_res.return_value

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_external_targets_data(self):
        external_targets_df = pd.read_excel(DataTestUnitMarsuae.external_targets)
        external_targets = self.mock_object('MARSUAE_SANDToolBox.get_all_kpi_external_targets')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('MARSUAE_SANDToolBox.get_kpi_result_values_df')
        kpi_result_value.return_value = DataTestUnitMarsuae.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_store_data(self):
        store_data = self.mock_object('MARSUAE_SANDToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitMarsuae.store_data_sss_a
        return store_data.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2')
        self.mock_object('DbUsers')
        self.mock_object('DbUsers', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider'), self.mock_object(
            'DbUsers')

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitMarsuae.kpi_static_data
        return static_kpi.return_value

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitMarsuae.test_case_1,
                                                                     sheetname='all_products')

    def mock_all_templates(self):
        self.data_provider_data_mock['all_templates'] = DataTestUnitMarsuae.all_templates

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_probe_group(self, data):
        probe_group = self.mock_object('MARSUAE_SANDToolBox.get_probe_group')
        probe_group.return_value = data.where(data.notnull(), None)
        return probe_group.return_value

    def create_scif_matches_stitch_groups_data_mocks(self, test_case_file_path, scenes_list):
        scif_test_case = pd.read_excel(test_case_file_path, sheetname='scif')
        matches_test_case = pd.read_excel(test_case_file_path, sheetname='matches')
        scif_scene = scif_test_case[scif_test_case['scene_fk'].isin(scenes_list)]
        matches_scene = matches_test_case[matches_test_case['scene_fk'].isin(scenes_list)]
        self.mock_scene_item_facts(scif_scene)
        self.mock_match_product_in_scene(matches_scene)
        probe_group = self.mock_probe_group(pd.read_excel(test_case_file_path, sheetname='stitch_groups'))
        return probe_group, matches_scene, scif_scene

    def mock_block(self):
        self.mock_object('Block')

    # def test_whatever(self):
    #     probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks(
    #         DataTestUnitMarsuae.test_case_1, [3])
    #     tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
    #     print tool_box.store_info_dict
    #     print tool_box.atomic_kpi_results
    #     print tool_box.lvl3_assortment

    def test_get_category_level_targets_returns_targets_for_lvl_2_kpis(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 3001, 'Weight': 70})
        expected_list.append({'kpi_level_2_fk': 3002, 'Weight': 20})
        expected_list.append({'kpi_level_2_fk': 3003, 'Weight': 10})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(tool_box.category_params, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(tool_box.category_params), 3)

    def test_get_kpi_category_df_matches_kpis_to_template_group(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        dict_list = tool_box.kpi_category_df.to_dict(orient='records')
        expected_list = list()
        expected_list.append({'KPI Level 2 Name': 'Chocolate & Ice Cream', 'template_group': 'Chocolate'})
        expected_list.append({'KPI Level 2 Name': 'Chocolate & Ice Cream', 'template_group': 'Ice Cream'})
        expected_list.append({'KPI Level 2 Name': 'Gum & Fruity', 'template_group': 'Gum and Confectionary'})
        expected_list.append({'KPI Level 2 Name': 'Pet Food', 'template_group': 'Pet food'})
        for expected_result in expected_list:
            self.assertTrue(expected_result in dict_list)
        self.assertEquals(len(tool_box.kpi_category_df), 4)

    def test_unpack_external_targets_json_fields_to_df_gets_all_fields_from_json_and_matches_correct_pks(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        expected_result = pd.DataFrame.from_records([
            {'pk': 2, u'store_att_name_1': u'store_type', u'store_att_value_1': u'SSS A'},
            {'pk': 3, u'store_att_name_1': u'store_type', u'store_att_value_1': u'Hypers'},
            {'pk': 10, u'store_att_name_1': u'store_type', u'store_att_value_1':
                    [u"SSS A", u"SSS B", u"Impulse A", u"Impulse B", u"Convenience A", u"Convenience B", u"Convenience C"]},
        ])
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_1, 'json_field')
        self.assertItemsEqual(['pk', 'store_att_name_1', 'store_att_value_1'], result_df.columns.values.tolist())
        assert_frame_equal(expected_result, result_df)

    def test_unpack_external_targets_json_fields_to_df_returns_empty_df_if_input_empty(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_empty,
                                                                       'json_field')
        self.assertTrue(result_df.empty)

    def test_unpack_external_targets_json_fields_to_df_returns_df_with_pk_field_only(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitMarsuae.data_json_empty_with_pks,
                                                                       'json_field')
        self.assertItemsEqual(result_df.columns.values.tolist(), ['pk'])

    def test_unpack_all_external_targets_forms_data_frame_with_all_relevant_columns_and_all_records(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        columns = tool_box.all_targets_unpacked.columns.values.tolist()
        expected_columns_in_output_df = DataTestUnitMarsuae.external_targets_columns
        validation_list = [col in columns for col in expected_columns_in_output_df]
        self.assertTrue(all(validation_list))
        self.assertEquals(len(tool_box.all_targets_unpacked), 40)

    def test_get_yes_no_result_type_fk_if_score_value_not_zero(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        expected_res = 2
        result_fk_distributed = tool_box.get_oos_distributed_result(1)
        self.assertEquals(result_fk_distributed, expected_res)

    def test_get_yes_no_result_type_fk_if_score_value_zero(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        expected_res = 1
        result_fk_oos = tool_box.get_oos_distributed_result(0)
        self.assertEquals(result_fk_oos, expected_res)

    def test_get_kpi_result_value_pk_by_value_returns_none_if_value_does_not_exist(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        result_fk = tool_box.get_kpi_result_value_pk_by_value('non_existing_value')
        self.assertIsNone(result_fk)

    def test_get_store_atomic_kpi_parameters_returns_atomics_relevant_to_store_in_session(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        expected_result = [55, 56, 57, 58, 59, 60, 61, 62, 63, 71, 72, 73, 74, 75, 76, 77, 78, 81, 82]
        store_atomics = tool_box.get_store_atomic_kpi_parameters()
        self.assertItemsEqual(store_atomics['pk'].values.tolist(), expected_result)

    def test_get_store_atomic_kpi_parameters_returns_empty_df_if_no_relevant_atomics_for_policy(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        tool_box.store_info_dict = DataTestUnitMarsuae.store_info_dict_other_type
        store_atomics = tool_box.get_store_atomic_kpi_parameters()
        self.assertTrue(store_atomics.empty)

    def test_get_atomics_for_template_groups_present_in_store_returns_both_choc_and_ice_cream_atomic_kpis(self):
        probe_group, matches, scene = self.create_scif_matches_stitch_groups_data_mocks(
                DataTestUnitMarsuae.test_case_1, [1, 2])
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        store_atomics = tool_box.get_store_atomic_kpi_parameters()
        store_atomics = tool_box.get_atomics_for_template_groups_present_in_store(store_atomics)
        expected_result = range(55, 64)
        self.assertItemsEqual(store_atomics['pk'].values.tolist(), expected_result)
        self.assertItemsEqual(store_atomics['KPI Level 2 Name'].unique().tolist(), ['Chocolate & Ice Cream'])

    def test_build_tiers_for_atomics(self):
        tool_box = MARSUAE_SANDToolBox(self.data_provider_mock, self.output)
        store_atomics = tool_box.get_store_atomic_kpi_parameters()
        tool_box.build_tiers_for_atomics(store_atomics)
        expected_kpi_list = ['NBL - Chocolate Main', 'NBL - Chocolate Checkout', 'NBL - Gum Main', 'NBL - Gum Checkout',
                             'NBL - Petfood Main', 'NBL - Ice Cream Main']
        self.assertItemsEqual(tool_box.atomic_tiers_df['kpi_type'].unique().tolist(), expected_kpi_list)
        expected_list = list()
        expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0, 'step_value': 0.83})
        expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0.5, 'step_value': 0.9})
        expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 0.8, 'step_value': 0.96})
        expected_list.append({'kpi_type': 'NBL - Chocolate Main', 'step_score_value': 1, 'step_value': 1.01})

        expected_list.append({'kpi_type': 'NBL - Gum Checkout', 'step_score_value': 0, 'step_value': 0.93})
        expected_list.append({'kpi_type': 'NBL - Gum Checkout', 'step_score_value': 1, 'step_value': 1.01})
        test_result_list = tool_box.atomic_tiers_df.to_dict(orient='records')
        for expected_result in expected_list:
            self.assertTrue(expected_result in test_result_list)



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