import pandas as pd
import datetime
from Projects.CCBR_PROD.Tests.Data import records
from Trax.Utils.Testing.Case import TestUnitCase, MagicMock
from Tests.TestUtils import remove_cache_and_storage
from Projects.CCBR_PROD.Utils.KPIToolBox import CCBRToolBox

__author__ = 'avrahama'


class TestCCCBR(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.CCBR_PROD.Utils.KPIToolBox.CCBRToolBox'

    def set_up(self):
        super(TestCCCBR, self).set_up()
        remove_cache_and_storage()

        # mock PSProjectConnector
        self.CCBRGENERALToolBox_mock = self.mock_object(
            'CCBRGENERALToolBox', path='Projects.CCBR_PROD.Utils.GeneralToolBox')
        self.SessionInfo_mock = self.mock_object(
            'SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')
        self.BaseCalculationsGroup_mock = self.mock_object(
            'BaseCalculationsGroup', path='Trax.Algo.Calculations.Core.Shortcuts')
        self.ProjectConnector_mock = self.mock_object(
            'Common', path='KPIUtils.DB.Common')
        self.ProjectConnector_mock = self.mock_object(
            'PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.PsDataProvider_mock = self.mock_object(
            'PsDataProvider', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        self.load_exel_to_df_mock = self.mock_object(
            'load_exel_to_df', path='Projects.CCBR_PROD.Utils.KPIToolBox.CCBRToolBox')

        # import sheets dfs
        self.load_exel_to_df_mock.side_effect = [pd.DataFrame(records.count_sheet),
                                                 pd.DataFrame(records.group_count_sheet),
                                                 pd.DataFrame(records.survey_sheet)]

        # mock 'data provider' and object
        self.data_provider_mock = MagicMock()
        self.output_mock = MagicMock()

        # create a dict of data_provider object relevant attributes
        self.my_dict = {'all_products': pd.DataFrame(records.all_products),
                        'store_info': pd.DataFrame(records.store_info),
                        'scene_item_facts': pd.DataFrame(records.scif),
                        'products': pd.DataFrame(records.products),
                        'session_info': pd.DataFrame(records.session_info),
                        'survey_responses': pd.DataFrame(records.survey_responses),
                        'visit_date': datetime.date(2018, 6, 11),
                        'scenes_info': pd.DataFrame({'template_fk': [3, 5, 6]}),
                        'all_templates': pd.DataFrame({'template_fk': [3, 5, 6]}),
                        'session_and_store_info': pd.DataFrame({'col1': [3, 5, 6]}),
                        'store_fk': 232
                        }
        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = self.my_dict.__getitem__
        self.data_provider_mock.__iter__.side_effect = self.my_dict.__iter__
        self.tool_box = CCBRToolBox(self.data_provider_mock, self.output_mock)

    def test_calculate_availability(self):
        activ_products = self.my_dict['all_products']
        activ_products = activ_products[activ_products.is_active == 1]
        self.tool_box.calculate_availability(activ_products)
        # test the actual results compered to expected results

    def test_calculate_pricing(self):
        pass

    def test_handle_survey_atomics(self):
        pass

    def test_handle_count_atomics(self):
        pass

    def test_handle_group_count_atomics(self):
        pass
