# -*- coding: utf-8 -*-
import json

from mock import MagicMock
# from pandas.util.testing import assert_frame_equal
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
        self.data_provider.session_uid = self.data.session_uid

        self.data_provider_data = self.data.data_provider_data

        def get_item(key):
            return self.data_provider_data[key] if key in self.data_provider_data else MagicMock()

        self.data_provider.__getitem__.side_effect = get_item

    def mock_tool_box(self):

        self.mock_object('CCRUKPIToolBox.rds_connection')
        self.mock_object('CCRUKPIToolBox.get_kpi_entities')
        self.mock_object('CCRUKPIToolBox.write_to_kpi_results_old')
        self.mock_object('CCRUKPIToolBox.write_to_kpi_facts_hidden')
        self.mock_object('CCRUKPIToolBox.get_pos_kpi_set_name')\
            .return_value = self.data.pos_kpi_set_name

        self.mock_object('Common')
        self.mock_object('SessionInfo')
        self.mock_object('PSProjectConnector')
        # self.mock_object('BaseCalculationsGroup')

        mock_fetcher = self.mock_object('CCRUCCHKPIFetcher')
        mock_fetcher.get_store_number = self.data.store_number
        mock_fetcher.get_test_store = self.data.test_store
        mock_fetcher.get_attr15_store = self.data.attr15_store
        mock_fetcher.get_store_area_df = self.data.store_areas.copy()
        mock_fetcher.get_session_user = self.data.session_user
        mock_fetcher.get_planned_visit_flag = self.data.planned_visit_flag

    def get_pos_test_case(self, test_case):
        test_parameters = self.data.pos_data[self.data.pos_data['test_case'] == test_case]
        test_result = test_parameters['test_result'].values[0]
        test_parameters = {0: json.loads(test_parameters.to_json(orient='records')),
                           1: [],
                           2: {'SESSION LEVEL': [], 'SCENE LEVEL': []}}
        return test_parameters, test_result

    def test_check_availability(self):
        test_cases = \
            [
                'test_availability_number_of_facings_1',
                'test_availability_number_of_facings_2',
                'test_availability_number_of_facings_3',
                'test_availability_number_of_facings_4'
            ]
        for test_case in test_cases:
            self.mock_data_provider()
            self.mock_tool_box()
            tool_box = CCRUKPIToolBox(self.data_provider, self.output)
            tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
            params, check_result = self.get_pos_test_case(test_case)
            test_result = tool_box.check_availability(params)
            self.assertEquals(check_result, test_result)

    def test_check_facings_sos(self):
        test_cases = \
            [
                'test_check_facings_sos_1'
            ]
        for test_case in test_cases:
            self.mock_data_provider()
            self.mock_tool_box()
            tool_box = CCRUKPIToolBox(self.data_provider, self.output)
            tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
            params, check_result = self.get_pos_test_case(test_case)
            test_result = tool_box.check_facings_sos(params)
            self.assertEquals(check_result, test_result)

    def test_check_share_of_cch(self):
        test_cases = \
            [
                'test_check_share_of_cch_1',
                'test_check_share_of_cch_2',
                'test_check_share_of_cch_3'
            ]
        for test_case in test_cases:
            self.mock_data_provider()
            self.mock_tool_box()
            tool_box = CCRUKPIToolBox(self.data_provider, self.output)
            tool_box.set_kpi_set(self.data.pos_kpi_set_name, self.data.pos_kpi_set_type)
            params, check_result = self.get_pos_test_case(test_case)
            test_result = tool_box.check_share_of_cch(params)
            self.assertEquals(check_result, test_result)

# writer = pd.ExcelWriter('./store_areas.xlsx', engine='xlsxwriter')
# self.store_areas.to_excel(writer, sheet_name='store_areas', index_label='#')
# writer.save()
# writer.close()