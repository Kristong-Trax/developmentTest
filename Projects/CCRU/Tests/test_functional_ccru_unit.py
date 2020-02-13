from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Projects.CCRU.Tests.Data.data_test_ccru_unit import DataTestUnitCCRU
from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data

from mock import MagicMock
import pandas as pd

from pandas.util.testing import assert_frame_equal


__author__ = 'sergey'


class TestCCRU(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Projects.CCRU.Utils.CCRUKPIToolBox'

    def set_up(self):
        super(TestCCRU, self).set_up()
        self.data = DataTestUnitCCRU()

        self.output = MagicMock()

        self.mock_data_provider()
        self.mock_tool_box()

    def mock_data_provider(self):
        self.data_provider = MagicMock()
        self.data_provider.project_name = self.data.project_name
        self.data_provider.session_uid = self.data.session_uid

        self.data_provider_data = self.data.data_provider_data

        def get_item(key):
            return self.data_provider_data[key] if key in self.data_provider_data else MagicMock()

        self.data_provider.__getitem__.side_effect = get_item

    def mock_tool_box(self):
        self.mock_object('rds_connection',
                         path='Projects.CCRU.Utils.ToolBox.CCRUKPIToolBox')
        self.mock_object('get_kpi_entities',
                         path='Projects.CCRU.Utils.ToolBox.CCRUKPIToolBox')

        self.mock_object('get_pos_kpi_set_name',
                         path='Projects.CCRU.Utils.ToolBox.CCRUKPIToolBox')\
            .return_value = self.data.pos_kpi_set_name

        self.mock_object('PSProjectConnector',
                         path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('Common',
                         path='KPIUtils_v2.DB.CommonV2')
        self.mock_object('SessionInfo',
                         path='Trax.Algo.Calculations.Core.Shortcuts')

        self.mock_object('CCRUCCHKPIFetcher',
                         path='Projects.CCRU.Utils.Fetcher')
        self.mock_object('CCRUCCHKPIFetcher.get_kpi_entity_types',
                         path='Projects.CCRU.Utils.Fetcher')

        self.mock_object('CCRUCCHKPIFetcher.get_external_session_id',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.external_session_id
        self.mock_object('CCRUCCHKPIFetcher.get_store_number',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.store_number
        self.mock_object('CCRUCCHKPIFetcher.get_test_store',
                         path='Projects.CCRU.Utils.Fetcher',
                         value=self.data.test_store)
        self.mock_object('CCRUCCHKPIFetcher.get_attr15_store',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.attr15_store
        self.mock_object('CCRUCCHKPIFetcher.get_store_area_df',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.store_areas
        self.mock_object('CCRUCCHKPIFetcher.get_session_user',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.session_user
        self.mock_object('CCRUCCHKPIFetcher.get_planned_visit_flag',
                         path='Projects.CCRU.Utils.Fetcher')\
            .return_value = self.data.planned_visit_flag

    def get_pos_test_case(self, test_case):
        test_parameters = self.data.pos_data[self.data.pos_data['test_case'] == test_case]
        test_result = test_parameters['test_result'].values[0]
        test_parameters = test_parameters.to_json(orient='records')
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
            params, check_result = self.get_pos_test_case(test_case)
            tool_box = CCRUKPIToolBox(self.data_provider, self.output)
            test_result = tool_box.check_availability(params)
            self.assertEquals(check_result, test_result)

# writer = pd.ExcelWriter('./store_areas.xlsx', engine='xlsxwriter')
# self.store_areas.to_excel(writer, sheet_name='store_areas', index_label='#')
# writer.save()
# writer.close()