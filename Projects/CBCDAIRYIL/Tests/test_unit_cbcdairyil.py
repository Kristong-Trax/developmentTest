# coding=utf-8
import os
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import MagicMock, mock
import pandas as pd
import numpy as np
from Projects.CBCDAIRYIL.Utils.KPIToolBox import CBCDAIRYILToolBox, Consts
from KPIUtils.ParseTemplates import parse_template


__author__ = 'idanr'


class TestConsts(object):
    RETAILER_FRIDGE = u'מקרר קמעונאי'
    OUT_CAT_FRIDGE = u'מקרר חוץ קטגוריה'


class TestCBCDAIRYIL(MockingTestCase):

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
        self.output = MagicMock()
        self.rds_conn = MagicMock()
        self.survey = self.mock_survey()
        self.block = self.mock_block()
        self.scif = self.mock_scif()
        self.general_toolbox = self.mock_general_toolbox()
        self.tool_box = CBCDAIRYILToolBox(self.data_provider_mock, MagicMock())

    def mock_scif(self):
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
        """

        """
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
            self.assertEqual(self.tool_box.calculate_kpi_result_by_weight(test_values),  expected_result)

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
            self.assertEqual(len(filtered_template),  expected_result)

    def test_scif_scenes_filters(self):
        """This test checks the scene filters by template_name and template_group"""
        params_1 = (pd.Series([TestConsts.RETAILER_FRIDGE, ''], index=['template_name', 'template_group']), [3])
        params_2 = (pd.Series([TestConsts.OUT_CAT_FRIDGE, ''], index=['template_name', 'template_group']), [1.2])
        params_3 = (pd.Series(['', ''], index=['template_name', 'template_group']), [1, 2, 3, 4])
        params_4 = (pd.Series(['', TestConsts.RETAILER_FRIDGE], index=['template_name', 'template_group']), [])
        test_cases = [params_1, params_2, params_3, params_4]
        for test_case_params, expected_result in test_cases:
            scenes_list = self.tool_box.get_relevant_scenes_by_params(test_case_params)
            self.assertEqual(scenes_list, expected_result)

    # def validate_template(self):
    #     problem = []
    #     atomics_df = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_SHEET, lower_headers_row_index=1)
    #     for i in atomics_df.index:
    #         current_atomic = atomics_df.loc[i]
    #         key = current_atomic[Consts.PARAMS_TYPE_1]
    #         if key in ['product_ean_code', 'brand_name', 'category']:
    #             value = current_atomic[Consts.PARAMS_VALUE_1]
    #             value = map(unicode.strip, value.split(Consts.SEPARATOR))
    #             for v in value:
    #                 filtered_df = self.all_products[self.all_products[key] == v]
    #                 if filtered_df.empty:
    #                     print "NO SUCH {} = {}".format(key, v)
    #                     problem.append(v)
    #         else:
    #             continue
    #     print problem
    #     return problem

