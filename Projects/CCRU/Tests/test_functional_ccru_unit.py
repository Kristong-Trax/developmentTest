# -*- coding: utf-8 -*-
import json

from mock import MagicMock
from pandas.util.testing import assert_frame_equal
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
# from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCRU.Tests.Data.data_test_ccru_unit import DataTestUnitCCRU
from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox


__author__ = 'sergey'


class TestCCRU(TestFunctionalCase):

    def set_up(self):
        super(TestCCRU, self).set_up()
        self.data = DataTestUnitCCRU()
        self.output = MagicMock()

    @property
    def import_path(self):
        return 'Projects.CCRU.Utils.ToolBox'

    def mock_data_provider(self):

        self.data_provider = MagicMock()
        self.data_provider.project_name = self.data.project_name
        self.data_provider.session_id = self.data.session_id
        self.data_provider.session_uid = self.data.session_uid

        self.data_provider_data = self.data.data_provider_data

        def get_item(key):
            return self.data_provider_data[key] if key in self.data_provider_data else MagicMock()

        self.data_provider.__getitem__.side_effect = get_item

    def mock_tool_box(self):

        self.mock_object('CCRUKPIToolBox.rds_connection')
        self.mock_object('CCRUKPIToolBox.write_to_kpi_results_old')
        self.mock_object('CCRUKPIToolBox.write_to_kpi_facts_hidden')
        self.mock_object('CCRUKPIToolBox.get_pos_kpi_set_name')\
            .return_value = self.data.pos_kpi_set_name

        # self.mock_object('Common')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

        self.mock_object('Common.get_kpi_fk_by_kpi_type') \
            .side_effect = self.common_get_kpi_fk_by_kpi_type
        self.mock_object('Common.get_kpi_static_data') \
            .return_value = self.data.kpi_level_2

        self.mock_object('SessionInfo')
        self.mock_object('PSProjectConnector')
        # self.mock_object('BaseCalculationsGroup')

        self.mock_object('CCRUCCHKPIFetcher.rds_connection')
        self.mock_object('CCRUCCHKPIFetcher.get_store_number')\
            .return_value = self.data.store_number
        self.mock_object('CCRUCCHKPIFetcher.get_test_store')\
            .return_value = self.data.test_store
        self.mock_object('CCRUCCHKPIFetcher.get_attr15_store')\
            .return_value = self.data.attr15_store
        self.mock_object('CCRUCCHKPIFetcher.get_store_area_df')\
            .return_value = self.data.store_areas.copy()
        self.mock_object('CCRUCCHKPIFetcher.get_session_user')\
            .return_value = self.data.session_user
        self.mock_object('CCRUCCHKPIFetcher.get_planned_visit_flag')\
            .return_value = self.data.planned_visit_flag
        self.mock_object('CCRUCCHKPIFetcher.get_top_skus_for_store') \
            .return_value = self.data.top_skus
        self.mock_object('CCRUCCHKPIFetcher.get_kpi_result_values') \
            .return_value = self.data.kpi_result_values.rename(columns={'pk': 'result_value_fk',
                                                                        'value': 'result_value',
                                                                        'kpi_result_type_fk': 'result_type_fk'})
        self.mock_object('CCRUCCHKPIFetcher.get_kpi_entity_types') \
            .return_value = self.data.kpi_entity_types
        self.mock_object('CCRUCCHKPIFetcher.get_kpi_entity') \
            .side_effect = self.fetcher_get_kpi_entity

    def fetcher_get_kpi_entity(self, entity, entity_type_fk, entity_table_name, entity_uid_field):
        if entity_table_name == 'pservice.group_names':
            df = self.data.group_names
            df['entity'] = entity
            df['type'] = entity_type_fk
            df['fk'] = df['pk']
            df['uid_field'] = df[entity_uid_field]
        elif entity_table_name == 'static_new.product':
            df = self.data.products
            df['entity'] = entity
            df['type'] = entity_type_fk
            df['fk'] = df['pk']
            df['uid_field'] = df[entity_uid_field]
        else:
            df = None
        return df

    def common_get_kpi_fk_by_kpi_type(self, kpi_name):
        return self.data.kpi_level_2[self.data.kpi_level_2['type'] == kpi_name]['pk'].values[0]

    def get_pos_test_case(self, test_case):
        test_parameters = self.data.pos_data[self.data.pos_data['test_case'] == test_case]
        test_result = test_parameters['test_result'].values[0]
        test_parameters = {0: json.loads(test_parameters.to_json(orient='records')),
                           1: [],
                           2: {'SESSION LEVEL': [], 'SCENE LEVEL': []}}
        return test_parameters, test_result

    def test_check_availability_1(self):
        test_case = 'test_check_availability_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_availability(params)
        self.assertEquals(check_result, test_result)

    def test_check_availability_2(self):
        test_case = 'test_check_availability_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_availability(params)
        self.assertEquals(check_result, test_result)

    def test_check_availability_3(self):
        test_case = 'test_check_availability_3'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_availability(params)
        self.assertEquals(check_result, test_result)

    def test_check_availability_4(self):
        test_case = 'test_check_availability_4'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_availability(params)
        self.assertEquals(check_result, test_result)

    def test_check_facings_sos_1(self):
        test_case = 'test_check_facings_sos_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_facings_sos(params)
        self.assertEquals(check_result, test_result)

    def test_check_share_of_cch_1(self):
        test_case = 'test_check_share_of_cch_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_share_of_cch(params)
        self.assertEquals(check_result, test_result)

    def test_check_share_of_cch_2(self):
        test_case = 'test_check_share_of_cch_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_share_of_cch(params)
        self.assertEquals(check_result, test_result)

    def test_check_share_of_cch_3(self):
        test_case = 'test_check_share_of_cch_3'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_share_of_cch(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_skus_per_door_range_1(self):
        test_case = 'test_check_number_of_skus_per_door_range_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_skus_per_door_range(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_skus_per_door_range_2(self):
        test_case = 'test_check_number_of_skus_per_door_range_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_skus_per_door_range(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_doors_1(self):
        test_case = 'test_check_number_of_doors_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_doors(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_1(self):
        test_case = 'test_check_number_of_scenes_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_2(self):
        test_case = 'test_check_number_of_scenes_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_3(self):
        test_case = 'test_check_number_of_scenes_3'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_4(self):
        test_case = 'test_check_number_of_scenes_4'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_5(self):
        test_case = 'test_check_number_of_scenes_5'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes(params)
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_no_tagging_1(self):
        test_case = 'test_check_number_of_scenes_no_tagging_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes_no_tagging(params)
        self.assertEquals(check_result, test_result)

    def test_check_customer_cooler_doors_1(self):
        test_case = 'test_check_customer_cooler_doors_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_customer_cooler_doors(params)
        self.assertEquals(check_result, test_result)

    def test_calculate_lead_sku_1(self):
        test_case = 'test_calculate_lead_sku_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        check_result = tuple([int(r) for r in check_result.split(', ')])
        test_result = tool_box.calculate_lead_sku(params[0][0])
        self.assertEquals(check_result, test_result)

    def test_calculate_lead_sku_2(self):
        test_case = 'test_calculate_lead_sku_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        check_result = tuple([int(r) for r in check_result.split(', ')])
        test_result = tool_box.calculate_lead_sku(params[0][0])
        self.assertEquals(check_result, test_result)

    def test_calculate_number_facings_near_food_1(self):
        test_case = 'test_calculate_number_facings_near_food_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.calculate_number_facings_near_food(params[0][0], params)
        self.assertEquals(check_result, test_result)

    def test_calculate_number_of_doors_more_than_target_facings_1(self):
        test_case = 'test_calculate_number_of_doors_more_than_target_facings_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.calculate_number_of_doors_more_than_target_facings(params[0][0])
        self.assertEquals(check_result, test_result)

    def test_check_number_of_doors_of_filled_coolers_1(self):
        test_case = 'test_check_number_of_doors_of_filled_coolers_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_doors_of_filled_coolers(params[0][0])
        self.assertEquals(check_result, test_result)

    def test_check_number_of_doors_of_filled_coolers_2(self):
        test_case = 'test_check_number_of_doors_of_filled_coolers_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        check_result = list([int(r) for r in check_result.split(', ')])
        test_result = tool_box.check_number_of_doors_of_filled_coolers(params[0][0], func='get scenes')
        self.assertEquals(check_result, test_result)

    def test_check_number_of_scenes_no_tagging_2(self):
        test_case = 'test_check_number_of_scenes_no_tagging_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_number_of_scenes_no_tagging(params[0][0], level=3)
        self.assertEquals(check_result, test_result)

    def test_calculate_number_of_scenes_with_target_1(self):
        test_case = 'test_calculate_number_of_scenes_with_target_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.calculate_number_of_scenes_with_target(params[0][0])
        self.assertEquals(check_result, test_result)

    def test_calculate_sub_atomic_passed_on_the_same_scene_1(self):
        test_case = 'test_calculate_sub_atomic_passed_on_the_same_scene_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.calculate_sub_atomic_passed_on_the_same_scene(params[0][0], params,
                                                                             scenes=[1, 2, 3], parent=MagicMock())
        self.assertEquals(check_result, test_result)

    def test_check_atomic_passed_1(self):
        test_case = 'test_check_atomic_passed_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_atomic_passed(params)
        self.assertEquals(check_result, test_result)

    def test_check_atomic_passed_on_the_same_scene_1(self):
        test_case = 'test_check_atomic_passed_on_the_same_scene_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_atomic_passed_on_the_same_scene(params)
        self.assertEquals(check_result, test_result)

    def test_check_sum_atomics_1(self):
        test_case = 'test_check_sum_atomics_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_sum_atomics(params)
        self.assertEquals(check_result, test_result)

    def test_check_weighted_average_1(self):
        test_case = 'test_check_weighted_average_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_weighted_average(params)
        self.assertEquals(round(check_result, 6), round(test_result, 6))

    def test_check_dummies_1(self):
        test_case = 'test_check_dummies_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_dummies(params)
        self.assertEquals(check_result, test_result)

    def test_check_kpi_scores_1(self):
        test_case = 'test_check_kpi_scores_1'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        tool_box.kpi_name_to_id[self.data.pos_kpi_set_type] = self.data.kpi_name_to_id
        tool_box.kpi_scores_and_results[self.data.pos_kpi_set_type] = self.data.kpi_scores_and_results
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_kpi_scores(params)
        self.assertEquals(check_result, test_result)

    def test_check_kpi_scores_2(self):
        test_case = 'test_check_kpi_scores_2'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
        tool_box.kpi_name_to_id[self.data.pos_kpi_set_type] = self.data.kpi_name_to_id
        tool_box.kpi_scores_and_results[self.data.pos_kpi_set_type] = self.data.kpi_scores_and_results
        params, check_result = self.get_pos_test_case(test_case)
        test_result = tool_box.check_kpi_scores(params)
        self.assertEquals(check_result, test_result)

    def test_calculate_top_sku(self):
        test_case = 'test_calculate_top_sku'
        self.mock_data_provider()
        self.mock_tool_box()
        tool_box = CCRUKPIToolBox(self.data_provider, self.output)
        tool_box.set_kpi_set(self.data.top_sku_kpi_set_name, self.data.top_sku_kpi_set_type)
        tool_box.kpi_name_to_id[self.data.pos_kpi_set_type] = self.data.kpi_name_to_id
        tool_box.kpi_scores_and_results[self.data.pos_kpi_set_type] = self.data.kpi_scores_and_results
        # params, check_result = self.get_pos_test_case(test_case)
        tool_box.calculate_top_sku(False, self.data.top_sku_kpi_set_name)
        # self.assertEquals(check_result, test_result)


# writer = pd.ExcelWriter('./results.xlsx', engine='xlsxwriter')
# self.store_areas.to_excel(writer, sheet_name='store_areas', index_label='#')
# writer.save()
# writer.close()