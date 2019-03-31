from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from Trax.Data.Testing.SeedNew import Seeder
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from mock import MagicMock
from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKCommonToolBox
from Projects.PEPSICOUK.Tests.data_test_unit_ccbza_sand import DataTestUnitPEPSICOUK
from Trax.Algo.Calculations.Core.DataProvider import Output
from pandas.util.testing import assert_frame_equal
import os
import pandas as pd
import numpy as np

__author__ = 'natalya'


class Test_PEPSICOUKCommon(MockingTestCase):
    # seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.Utils.CommonToolBox'

    def set_up(self):
        # super(Test_PEPSICOUK, self).setUp()
        super(Test_PEPSICOUKCommon, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.db_users_mock = self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()

        self.custom_entity_data_mock = self.mock_custom_entity_data()
        self.on_display_products_mock = self.mock_on_display_products()

        self.exclusion_template_mock = self.mock_template_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        self.mock_all_products()
        self.mock_all_templates()

    def mock_db_users(self):
        return self.mock_object('DbUsers',
                                path='KPIUtils_v2.DB.CommonV2')

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
        # return custom_entities

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitPEPSICOUK.kpi_static_data
        return static_kpi.return_value
        # return static_kpi

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value
        # return template_data_mock

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

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_all_products_in_data_provider(self, data):
        self.data_provider_data_mock['all_products'] = data.where(data.notnull(), None)

    # def test_unpack_external_targets_json_fields_to_df_gets_all_fields_from_json_and_matches_correct_pks(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    #     expected_result = pd.DataFrame.from_records([
    #         {'pk': 2, u'store_type': u'CORE', u'additional_attribute_2': np.nan, u'additional_attribute_1': u'ALL'},
    #         {'pk': 3, u'store_type': u'ALL', u'additional_attribute_2': np.nan, u'additional_attribute_1': u'OT'},
    #         {'pk': 10, u'store_type': u'CORE', u'additional_attribute_2': u'SAINSBURY', u'additional_attribute_1': np.nan},
    #     ])
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_1, 'json_field')
    #     self.assertItemsEqual(['pk', 'store_type', 'additional_attribute_1','additional_attribute_2'],
    #                           result_df.columns.values.tolist())
    #     assert_frame_equal(expected_result, result_df)
    #
    # def test_unpack_external_targets_json_fields_to_df_returns_empty_df_if_input_empty(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_empty,
    #                                                                    'json_field')
    #     self.assertTrue(result_df.empty)
    #
    # def test_unpack_external_targets_json_fields_to_df_returns_df_with_pk_field_only(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    #     result_df = tool_box.unpack_external_targets_json_fields_to_df(DataTestUnitPEPSICOUK.data_json_empty_with_pks,
    #                                                                    'json_field')
    #     self.assertItemsEqual(result_df.columns.values.tolist(), ['pk'])
    #
    # # def test_filtered_matches(self):
    # #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    # #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    # #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    # #     print tool_box.match_product_in_scene
    # #     print tool_box.filtered_matches
    # #
    # # def test_filtered_scif(self):
    # #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    # #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    # #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    # #     print tool_box.scif
    # #     print tool_box.filtered_scif
    #
    # def test_filters_for_scif_and_matches_are_retrieved_in_the_right_format(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     tool_box = PEPSICOUKCommonToolBox(self.data_provider_mock, self.output)
    #     expected_result = {'smart_attribute_state': (['additional display'], 0),
    #                        'location_type': ['Primary Shelf'],
    #                        'product_name': (['General Empty'], 0)}
    #     template_filters = tool_box.get_filters_dictionary(tool_box.exclusion_template)
    #     self.assertDictEqual(expected_result, template_filters)

        # # print excl_template_all_kpis
        # print template_filters
        # # product_keys = filter(lambda x: x in tool_box.all_products.columns.values.tolist(),
        # #                       template_filters.keys())
        # # print product_keys
        # filters = tool_box.get_filters_for_scif_and_matches(template_filters)
        # print filters
        # # print tool_box.all_products