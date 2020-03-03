# coding=utf-8
import os
import math

import numpy
import pandas as pd
from mock import MagicMock
from KPIUtils.ParseTemplates import parse_template
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Projects.CBCDAIRYIL.Utils.KPIToolBox import CBCDAIRYILToolBox, Consts


__author__ = 'idanr'


class TestConsts(object):
    RETAILER_FRIDGE = u'מקרר קמעונאי'
    OUT_CAT_FRIDGE = u'מקרר חוץ קטגוריה'
    PROJECT_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
    KPI_EYE_LEVEL = u'האם גבינה צהובה נמצאת בגובה העיניים (מדפים 3-4)'
    KPI_BATH = u'האם מאגדות נמצאות באמבטיה'


def get_test_case_template_all_tests():
    test_case_xls_object = pd.ExcelFile(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                                     'test_case_data.xlsx'))
    return test_case_xls_object

def get_scif_matches_stich_groups(xls_file):
    scif_test_case = pd.read_excel(xls_file, sheetname='scif', dtype={'product_ean_code': str})
    matches_test_case = pd.read_excel(xls_file, sheetname='matches')
    probe_groups = pd.read_excel(xls_file, sheetname='stitch_groups')
    return scif_test_case, matches_test_case, probe_groups


class TestCBCDAIRYIL(TestFunctionalCase):
    TEST_CASE = get_test_case_template_all_tests()
    SCIF, MATCHES, PROBE_GROUPS = get_scif_matches_stich_groups(TEST_CASE)

    @property
    def import_path(self):
        return 'Projects.CBCDAIRYIL.Utils.KPIToolBox'

    def set_up(self):
        super(TestCBCDAIRYIL, self).set_up()
        self.mock_data_provider()
        # self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'Test_Project_1'
        # self.data_provider_mock.rds_conn = MagicMock()
        self.PSProjectConnector = self.mock_object('PSProjectConnector',
                                                   path='KPIUtils_v2.DB.PsProjectConnector')
        self.common = self.mock_common()
        self.old_common = self.mock_object('oldCommon')
        self.output = MagicMock()
        self.rds_conn = MagicMock()
        self.survey = self.mock_survey()
        self.block = self.mock_block()
        self.scif = self.get_made_up_scif()
        # self.general_toolbox = self.mock_general_toolbox()
        self.mock_parse_template_data()
        self.tool_box = CBCDAIRYILToolBox(self.data_provider_mock, MagicMock())

    def mock_parse_template_data(self):
        template_mock = self.mock_object('CBCDAIRYILToolBox.parse_template_data')
        template_data = parse_template(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                                   'test_template.xlsx'), Consts.KPI_SHEET, lower_headers_row_index=1)
        template_mock.return_value = template_data


    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item


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

    @staticmethod
    def get_made_up_scif():
        d = {'scene_id': [1, 2, 3, 4],
             'template_name': [TestConsts.OUT_CAT_FRIDGE, TestConsts.OUT_CAT_FRIDGE, TestConsts.RETAILER_FRIDGE,
                               'SOME_TEMPLATE'], 'template_group': ['', '', '', '']}
        df = pd.DataFrame(data=d)
        return df

    def mock_common(self):
        return self.mock_object('Common')

    def mock_survey(self):
        return self.mock_object('Survey')

    def mock_block(self):
        return self.mock_object('Block')

    def mock_general_toolbox(self):
        return self.mock_object('GENERALToolBox')

    def test_current_project_template(self):
        """ This test is check the validation of the current project's template! """
        # Columns tests
        expected_columns = {'Atomic Name', 'KPI Name', 'KPI Set', 'store_type', 'additional_attribute_1',
                            'additional_attribute_2', 'additional_attribute_3', 'additional_attribute_6',
                            'additional_attribute_7', 'Template Name', 'Template group',
                            'KPI Family', 'Score Type', 'Param Type (1)/ Numerator', 'Param (1) Values',
                            'Param Type (2)/ Denominator', 'Param (2) Values', 'Param Type (3)', 'Param (3) Values',
                            'Weight', 'Target', 'Split Score'}
        template = pd.read_excel(TestConsts.PROJECT_TEMPLATE_PATH, skiprows=1)
        if expected_columns.difference(template.columns):
            # Gives it another shot - Maybe the redundant top row was removed
            template = pd.read_excel(TestConsts.PROJECT_TEMPLATE_PATH, skiprows=0)
        self.assertEqual(set(), expected_columns.difference(template.columns),
                         msg="The template's columns are different than the expected ones!")

        # Template's attributes Test
        self.assertTrue(self._check_template_instances(template), msg="One of template's attributes has nan value!")

    @staticmethod
    def _check_template_instances(template):
        columns_to_check = ['additional_attribute_6', 'Weight',
                            'KPI Family', 'KPI Set']
        for col in columns_to_check:
            attribute_values = template[col].unique().tolist()
            for value in attribute_values:
                if isinstance(value, float) and math.isnan(value):
                    print "ERROR! There is a empty value in the following column: {}".format(col)
                    return False
        return True

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

    def _get_param_series_by_atomic_name(self, df, atomic_name):
        df = df.loc[df['Atomic Name'].str.encode('utf-8') == atomic_name.encode('utf-8')]
        if not df.empty:
            df = df.iloc[0]

        return df

    def test_calculate_eye_level_passes_if_75_percent_product_on_eye_level(self):
        expected_case_result = 100
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_EYE_LEVEL)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            num_result, denominator_result, atomic_score = toolbox.calculate_eye_level(**general_filters)
            self.assertEquals(atomic_score, expected_case_result)
        else:
            self.assertFalse(series.empty, True)

    def test_calculate_eye_level_fails_if_not_75_percent_product_on_eye_level(self):
        expected_case_result = 66.67
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([2])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_EYE_LEVEL)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            num_result, denominator_result, atomic_score = toolbox.calculate_eye_level(**general_filters)
            self.assertEquals(round(atomic_score, 2), expected_case_result)
        else:
            self.assertFalse(series.empty, True)

    def test_calculate_eye_level_fails_if_no_product_on_eye_level(self):
        expected_case_result = 0
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([3])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_EYE_LEVEL)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            num_result, denominator_result, atomic_score = toolbox.calculate_eye_level(**general_filters)
            self.assertEquals(round(atomic_score, 2), expected_case_result)
        else:
            self.assertFalse(series.empty, True)

    def test_calculate_eye_level_fails_if_no_products(self):
        expected_case_result = 0
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([4])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_EYE_LEVEL)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            num_result, denominator_result, atomic_score = toolbox.calculate_eye_level(**general_filters)
            self.assertEquals(round(atomic_score, 2), expected_case_result)
        else:
            self.assertFalse(series.empty, True)

    def test_availability_from_bottom_fails_if_product_not_in_bath_exist(self):
        expected_case_result = 0
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([5])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_BATH)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            res = toolbox.calculate_availability_from_bottom(**general_filters)
            self.assertEquals(res, expected_case_result)

    def test_availability_from_bottom_passes_if_all_products_in_bath(self):
        expected_case_result = 100
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([6])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_BATH)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            res = toolbox.calculate_availability_from_bottom(**general_filters)
            self.assertEquals(res, expected_case_result)

    def test_availability_from_bottom_fails_if_no_products_in_bath(self):
        expected_case_result = 0
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([7])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_BATH)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            res = toolbox.calculate_availability_from_bottom(**general_filters)
            self.assertEquals(res, expected_case_result)

    def test_availability_from_bottom_passes_if_products_in_bath_ignore_others(self):
        expected_case_result = 100
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([8])
        toolbox = CBCDAIRYILToolBox(self.data_provider_mock, self.output)
        series = self._get_param_series_by_atomic_name(toolbox.template_data, TestConsts.KPI_BATH)
        if series.size != 0:
            general_filters = toolbox.get_general_filters(series)
            res = toolbox.calculate_availability_from_bottom(**general_filters)
            self.assertEquals(res, expected_case_result)