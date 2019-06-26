# coding=utf-8
import os
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Utils.Conf.Configuration import Config
# import numpy as np
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
import pandas as pd
from Projects.CBCDAIRYIL.Utils.KPIToolBox import CBCDAIRYILToolBox, Consts
from KPIUtils.ParseTemplates import parse_template


__author__ = 'idanr'


class TestConsts(object):
    RETAILER_FRIDGE = u'מקרר קמעונאי'
    OUT_CAT_FRIDGE = u'מקרר חוץ קטגוריה'


class TestCBCDAIRYIL(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Projects.CBCDAIRYIL.Utils.KPIToolBox'

    def set_up(self):
        super(TestCBCDAIRYIL, self).set_up()
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.project_connector_mock = self.mock_project_connector()
        self.common = self.mock_common()
        self.old_common = self.mock_object('oldCommon')
        self.output = MagicMock()
        self.rds_conn = MagicMock()
        self.survey = self.mock_survey()
        self.block = self.mock_block()
        self.scif = self.get_made_up_scif()
        self.general_toolbox = self.mock_general_toolbox()
        self.tool_box = CBCDAIRYILToolBox(self.data_provider_mock, MagicMock())

    @staticmethod
    def get_made_up_scif():
        d = {'scene_id': [1, 2, 3, 4],
             'template_name': [TestConsts.OUT_CAT_FRIDGE, TestConsts.OUT_CAT_FRIDGE, TestConsts.RETAILER_FRIDGE,
                               'SOME_TEMPLATE'], 'template_group': ['', '', '', '']}
        df = pd.DataFrame(data=d)
        return df

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_common(self):
        return self.mock_object('Common')

    def mock_survey(self):
        return self.mock_object('Survey')

    def mock_block(self):
        return self.mock_object('Block')

    def mock_general_toolbox(self):
        return self.mock_object('GENERALToolBox')

    def test_kpi_percentage_games(self):
        """ This is a test for the weights' games in Tara"""
        regular_percentage_1 = ([(100, 0.3), (100, 0.4), (0, 0.3)], 70)
        regular_percentage_2 = ([(50, 0.2), (10, 0.4), (0, 0.1), (0, 0.3)],  14)
        missing_percentage_1 = ([(100, 0.5), (0, 0.3)], 60)
        missing_percentage_2 = ([(100, 0.1), (0, 0.1)], 50)
        missing_percentage_3 = ([(50, 0.3), (20, 0.2)], 36.5)
        percentage_with_nones_1 = ([(100, 0.5), (0, None), (100, None), (100, 0.4)], 75)
        percentage_with_nones_2 = ([(10, 0.5), (0, None), (10, 0.1), (10, 0.1), (10, None)], 8)
        test_cases_list = [regular_percentage_1, regular_percentage_2, missing_percentage_1, missing_percentage_2,
                           missing_percentage_3, percentage_with_nones_1, percentage_with_nones_2]
        for test_values, expected_result in test_cases_list:
            self.assertEqual(expected_result, self.tool_box.calculate_kpi_result_by_weight(test_values, 1.0, False))

    def test_template_filters(self):
        """
        This test checks for all of the template filtering functions.
        """

        test_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                          Consts.PROJECT_TEMPLATE_NAME)
        kpis_sheet = parse_template(test_template_path, Consts.KPI_SHEET, lower_headers_row_index=1)
        store_attr_test_1 = ({Consts.STORE_TYPE: Consts.DYNAMO, Consts.ADDITIONAL_ATTRIBUTE_1: Consts.GENERAL,
                              Consts.ADDITIONAL_ATTRIBUTE_2: Consts.HEBREW_YES,
                              Consts.ADDITIONAL_ATTRIBUTE_3: Consts.HEBREW_YES}, 26)
        store_attr_test_2 = ({Consts.STORE_TYPE: Consts.MINI_MARKET, Consts.ADDITIONAL_ATTRIBUTE_1: Consts.ARAB,
                              Consts.ADDITIONAL_ATTRIBUTE_2: Consts.HEBREW_YES,
                              Consts.ADDITIONAL_ATTRIBUTE_3: Consts.HEBREW_YES}, 16)
        store_attr_test_3 = ({Consts.STORE_TYPE: Consts.DYNAMO, Consts.ADDITIONAL_ATTRIBUTE_1: Consts.GENERAL,
                              Consts.ADDITIONAL_ATTRIBUTE_2: Consts.HEBREW_NO,
                              Consts.ADDITIONAL_ATTRIBUTE_3: Consts.HEBREW_NO}, 30)
        store_attr_test_4 = ({Consts.STORE_TYPE: Consts.MINI_MARKET, Consts.ADDITIONAL_ATTRIBUTE_1: Consts.ARAB,
                              Consts.ADDITIONAL_ATTRIBUTE_2: Consts.HEBREW_NO,
                              Consts.ADDITIONAL_ATTRIBUTE_3: Consts.HEBREW_NO}, 20)
        store_attr_test_5 = ({Consts.STORE_TYPE: Consts.DYNAMO, Consts.ADDITIONAL_ATTRIBUTE_1: Consts.ORTHODOX,
                              Consts.ADDITIONAL_ATTRIBUTE_2: Consts.HEBREW_YES,
                              Consts.ADDITIONAL_ATTRIBUTE_3: Consts.HEBREW_YES}, 24)
        test_cases_list = [store_attr_test_1, store_attr_test_2, store_attr_test_3, store_attr_test_4,
                           store_attr_test_5]
        for test_values, expected_result in test_cases_list:
            filtered_template = self.tool_box.filter_template_by_store_att(kpis_sheet, test_values)
            self.assertEqual(expected_result, len(filtered_template))

    def test_scif_scenes_filters(self):
        """This test checks the scene filters by template_name and template_group"""
        params_1 = (
            pd.Series([TestConsts.RETAILER_FRIDGE, ''], index=[Consts.TEMPLATE_NAME, Consts.TEMPLATE_GROUP]), [3])
        params_2 = (
            pd.Series([TestConsts.OUT_CAT_FRIDGE, ''], index=[Consts.TEMPLATE_NAME, Consts.TEMPLATE_GROUP]), [1, 2])
        params_3 = (pd.Series(['', ''], index=[Consts.TEMPLATE_NAME, Consts.TEMPLATE_GROUP]), [1, 2, 3, 4])
        params_4 = (
            pd.Series(['', TestConsts.RETAILER_FRIDGE], index=[Consts.TEMPLATE_NAME, Consts.TEMPLATE_GROUP]), [])
        test_cases = [params_1, params_2, params_3, params_4]
        for test_case_params, expected_result in test_cases:
            self.tool_box.scif = self.get_made_up_scif()
            scenes_list = self.tool_box.get_relevant_scenes_by_params(test_case_params)
            self.assertEqual(expected_result, scenes_list)

    def test_get_number_of_facings_per_product_dict(self):
        """ This is a test for get_number_of_facings_per_product_dict function"""
        my_df = {Consts.EAN_CODE: [1, 2, 3, 4], Consts.FACINGS: [2, 2, 3, 3], Consts.FACINGS_IGN_STACK: [1, 1, 0, 0]}
        my_df = pd.DataFrame(data=my_df)
        dict_add_stack = self.tool_box.get_number_of_facings_per_product_dict(my_df, ignore_stack=False)
        dict_ignore_stack = self.tool_box.get_number_of_facings_per_product_dict(my_df, ignore_stack=True)
        self.assertEqual(4, len(dict_add_stack))
        self.assertEqual(2, len(dict_ignore_stack))
        self.assertEqual([1, 2, 3, 4], dict_add_stack.keys())
        self.assertEqual([1, 2], dict_ignore_stack.keys())

    # def test_filter_df_by_shelves(self):
    #     df_3_shelves = {Consts.SCENE_FK: [1, 1, 2, 2, 2, 3, 3, 4, 4, 4],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [1, 1, 2, 2, 2, 3, 3, 4, 4, 4]}
    #     df_4_shelves = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4]}
    #     df_5_shelves = {Consts.SCENE_FK: [1, 2, 3, 4, 5],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [5, 4, 3, 2, 1]}
    #     df_6_shelves = {Consts.SCENE_FK: [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [6, 5, 4, 4, 3, 3, 3, 2, 2, 3, 2, 1]}
    #     df_7_shelves = {Consts.SCENE_FK: [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [7, 6, 6, 7, 3, 3, 4, 4, 5, 5, 1, 2]}
    #     df_15_shelves = {Consts.SCENE_FK: [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 7, 7, 7],
    #                      Consts.SHELF_NUM_FROM_BOTTOM: [15, 14, 14, 12, 13, 14, 13, 13, 14, 5, 3, 2, 5, 5, 5, 1, 2, 2,
    #                                                     1, 1, 2]}
    #     df_2_shelves = {Consts.SCENE_FK: [1, 1, 2, 2],
    #                     Consts.SHELF_NUM_FROM_BOTTOM: [1, 2, 1, 2]}
    #     df_no_shelves = {Consts.SCENE_FK: [], Consts.SHELF_NUM_FROM_BOTTOM: []}
    #     potential_dfs_list = [(df_3_shelves, [2, 3, 4]), (df_4_shelves, [2]), (df_5_shelves, [2, 3]),
    #                           (df_6_shelves, [1, 2, 3, 4, 5]),
    #                           (df_7_shelves, [3, 4, 5]),
    #                           (df_15_shelves, [2, 3, 4, 5]), (df_2_shelves, [1, 2]), (df_no_shelves, [])]
    #     for d, expected in potential_dfs_list:
    #         df = pd.DataFrame(data=d)
    #         filtered_df = self.tool_box.filter_df_by_shelves(df, df, Consts.EYE_LEVEL_PER_SHELF)
    #         scenes = filtered_df['scene_fk'].unique().tolist()
    #         self.assertEqual(expected, scenes)

    def test_filter_df_by_shelves_top_bottom_max_match(self):
        df_3_shelves = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1],
                        Consts.SHELF_NUM_FROM_BOTTOM: [1, 1, 2, 2, 2, 3, 3, 3],
                        Consts.SHELF_NUM: [3, 3, 2, 2, 2, 1, 1, 1]}
        merged_df_3 = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1],
                       Consts.SHELF_NUM_FROM_BOTTOM: [1, 2, 2, 2, 3, 3],
                       Consts.SHELF_NUM: [3, 2, 2, 2, 1, 1]}
        df_no_shelves = {Consts.SCENE_FK: [], Consts.SHELF_NUM_FROM_BOTTOM: [], Consts.SHELF_NUM: []}
        merged_df_no_shelves = {Consts.SCENE_FK: [], Consts.SHELF_NUM_FROM_BOTTOM: [], Consts.SHELF_NUM: []}
        df_2_shelves = {Consts.SCENE_FK: [1, 1, 2, 2],
                        Consts.SHELF_NUM_FROM_BOTTOM: [1, 2, 1, 2],
                        Consts.SHELF_NUM: [2, 1, 2, 1]}
        merged_df_2 = {Consts.SCENE_FK: [1, 1],
                       Consts.SHELF_NUM_FROM_BOTTOM: [1, 2],
                       Consts.SHELF_NUM: [2, 1]}
        df_2_shelves_2 = {Consts.SCENE_FK: [1, 1, 2, 2],
                          Consts.SHELF_NUM_FROM_BOTTOM: [1, 2, 1, 2],
                          Consts.SHELF_NUM: [2, 1, 2, 1]}
        merged_df_2_2 = {Consts.SCENE_FK: [1],
                         Consts.SHELF_NUM_FROM_BOTTOM: [1],
                         Consts.SHELF_NUM: [2]}
        df_6_shelves = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                        Consts.SHELF_NUM_FROM_BOTTOM: [6, 5, 4, 4, 3, 3, 3, 2, 2, 3, 2, 1],
                        Consts.SHELF_NUM: [1, 2, 3, 3, 4, 4, 4, 5, 5, 4, 5, 6]}
        merged_df_6 = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                       Consts.SHELF_NUM_FROM_BOTTOM: [6, 5, 4, 4, 3, 3, 3, 2, 2, 3, 2, 1],
                       Consts.SHELF_NUM: [1, 2, 3, 3, 4, 4, 4, 5, 5, 4, 5, 6]}
        df_8_shelves = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                        Consts.SHELF_NUM_FROM_BOTTOM: [8, 7, 7, 6, 6, 5, 7, 4, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1],
                        Consts.SHELF_NUM: [1, 2, 2, 3, 3, 4, 2, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8]}
        merged_df_8 = {Consts.SCENE_FK: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                       Consts.SHELF_NUM_FROM_BOTTOM: [8, 7, 6, 5, 7, 4, 4, 4, 4, 3, 3, 3, 2, 2, 2, 1],
                       Consts.SHELF_NUM: [1, 2, 3, 4, 2, 5, 5, 5, 5, 6, 6, 6, 7, 7, 7, 8]}
        potential_dfs_list = [(merged_df_3, df_3_shelves, [1, 2], 5), (merged_df_no_shelves, df_no_shelves, [], 0),
                              (merged_df_2, df_2_shelves, [1, 2], 2), (merged_df_2_2, df_2_shelves_2, [2], 1),
                              (merged_df_6, df_6_shelves, [2, 3, 4], 7), (merged_df_8, df_8_shelves, [3, 4, 5, 6], 9)]
        for d, d_full, expected_shelves, exp_length in potential_dfs_list:
            df = pd.DataFrame(data=d)
            df_full = pd.DataFrame(data=d_full)
            filtered_df = self.tool_box.filter_df_by_shelves(df, df_full, Consts.EYE_LEVEL_PER_SHELF)
            eye_lvl_shelves = filtered_df[Consts.SHELF_NUM].unique().tolist()
            self.assertItemsEqual(expected_shelves, eye_lvl_shelves)
            self.assertEqual(exp_length, len(filtered_df))
