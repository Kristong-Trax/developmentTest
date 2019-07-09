import pandas as pd
import datetime
from Projects.CCBR_PROD.Tests.Data import records
from Projects.CCBR_PROD.Tests.Data import kpis
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

        # send templates as DFs
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
                        'store_fk': 232,
                        'active_products': pd.DataFrame(records.active_products)
                        }
        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = self.my_dict.__getitem__
        self.data_provider_mock.__iter__.side_effect = self.my_dict.__iter__
        self.tool_box = CCBRToolBox(self.data_provider_mock, self.output_mock)

    @staticmethod
    def sum_results(kpi_results):
        sum_results = 0
        for i in range(0, len(kpi_results)):
            sum_results = sum_results + float(kpi_results[i][2]['result'])
        return sum_results

    def test_calculate_availability_results(self):
        """
        test this that calculate_availability KPI results is the same as the results
        captured for this KPI with current templates
        """
        self.tool_box.write_to_db_result_new_tables = MagicMock()
        # get data for the method input
        active_products = self.my_dict['active_products']
        self.tool_box.calculate_availability(active_products)
        # test the actual results compered to expected results
        kpi_results = self.tool_box.write_to_db_result_new_tables.mock_calls
        # recorded results
        expected_results = 20.0
        actual_results = self.sum_results(kpi_results)
        # test if the same DFs are equal
        self.assertEqual(actual_results, expected_results)

    def test_calculate_pricing_results(self):
        """
        test this that calculate_pricing KPI results (sum) is the same as the results (sum)
        captured for this KPI with current templates
        """
        self.tool_box.write_to_db_result_new_tables = MagicMock()
        self.tool_box.prices_per_session = pd.DataFrame(records.prices_per_session)
        self.tool_box.calculate_pricing(pd.DataFrame(records.all_products_pricing))
        kpi_results = self.tool_box.write_to_db_result_new_tables.mock_calls
        expected_results = 212.36
        actual_results = self.sum_results(kpi_results)
        self.assertEqual(actual_results, expected_results)

    def test_handle_atomics_KPIs_results(self):
        # self.tool_box.write_to_db_result_new_tables = MagicMock()
        kpis_sheet = pd.DataFrame(kpis.kpi_sheet)

        kpis_sheet_filterd = kpis_sheet[kpis_sheet['KPI Type'] == 'SURVEY']

        for index, row in kpis_sheet_filterd.iterrows():
            atomic_name = row['English KPI Name'].strip()
            self.tool_box.write_to_db_result_new_tables = MagicMock()
            self.tool_box.handle_survey_atomics(atomic_name)
            kpi_results = self.tool_box.write_to_db_result_new_tables.mock_calls
            print kpi_results



            # self.tool_box.write_to_db_result_new_tables = MagicMock()
        # self.tool_box.handle_survey_atomics(atomic_name)
        # kpi_results = self.tool_box.write_to_db_result_new_tables.mock_calls
        # expected_results = 1
        # actual_results = self.sum_results(kpi_results)
        # self.assertEqual(expected_results, actual_results)

    def test_handle_count_atomics(self):
        pass

    def test_handle_group_count_atomics(self):
        pass
