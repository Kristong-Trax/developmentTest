from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, PropertyMock
from Projects.RISPARKWINEDE.Tests.data_test_unit_rmsk import DataTestUnitRmsk
from Projects.RISPARKWINEDE.Utils.KPIToolBox import RISPARKWINEDEToolBox
import pandas as pd
# import numpy as np
from pandas.util.testing import assert_frame_equal
from dateutil import parser

__author__ = 'natalyak'


def get_test_case_template_all_tests():
    test_case_xls_object = pd.ExcelFile(DataTestUnitRmsk.test_case)
    return test_case_xls_object


def get_scif_matches_stich_groups(xls_file):
    scif_test_case = pd.read_excel(xls_file, sheetname='scif')
    matches_test_case = pd.read_excel(xls_file, sheetname='matches')
    lvl3_result = pd.read_excel(xls_file, sheetname='lvl3_result')
    store_assortment = pd.read_excel(xls_file, sheetname='store_assortment')
    return scif_test_case, matches_test_case, lvl3_result, store_assortment


class TestMarsuae(TestFunctionalCase):

    TEST_CASE = get_test_case_template_all_tests()
    SCIF, MATCHES, LVL3_RESULT, STORE_ASSORTMENT = get_scif_matches_stich_groups(TEST_CASE)

    @property
    def import_path(self):
        return 'Projects.RISPARKWINEDE.Utils.KPIToolBox'

    def set_up(self):
        super(TestMarsuae, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.mock_all_products()
        self.mock_various_project_connectors()
        self.mock_position_graph()
        self.mock_all_scenes_in_session(pd.DataFrame(columns=['scene_fk', 'template_fk', 'template_group']))
        self.mock_visit_date_property('2019-12-20')

    def mock_all_scenes_in_session(self, data):
        self.data_provider_data_mock['scenes_info'] = data

    def mock_lvl3_ass_base_df(self, data):
        ass_res = self.mock_object('Assortment.calculate_lvl3_assortment',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = data

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

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
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitRmsk.kpi_static_data
        return static_kpi.return_value

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(self.TEST_CASE,
                                                                     sheetname='all_products')

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_visit_date_property(self, date_str):
        date = parser.parse(date_str).date()
        self.data_provider_data_mock['visit_date'] = date

    def create_assortment_result_data_mock(self, test_case_number):
        lvl3_result_test_case = self.LVL3_RESULT
        lvl3_result = lvl3_result_test_case[lvl3_result_test_case['test_case_number'].isin(test_case_number)]
        lvl3_result.drop('test_case_number', axis=1, inplace=True)
        self.mock_lvl3_ass_base_df(lvl3_result)
        return lvl3_result

    def test_wine_assortment_calculation_no_wine_products_in_session(self):
        lvl3_result = self.create_assortment_result_data_mock([1])
        tool_box = RISPARKWINEDEToolBox(self.data_provider_mock, self.output)
        tool_box.store_assortment = self.STORE_ASSORTMENT
        tool_box.common.write_to_db_result = MagicMock()
        tool_box.wine_assortment_calculation(lvl3_result)
        results_df = self.build_results_df(tool_box)
        self.assertEquals(len(results_df), 4)

        expected_list = list()
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 6, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 7, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 8, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 9, 'denominator_result': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_wine_assortment_calculation_wine_products_present_in_session(self):
        lvl3_result = self.create_assortment_result_data_mock([2])
        tool_box = RISPARKWINEDEToolBox(self.data_provider_mock, self.output)
        tool_box.store_assortment = self.STORE_ASSORTMENT
        tool_box.common.write_to_db_result = MagicMock()
        tool_box.wine_assortment_calculation(lvl3_result)
        results_df = self.build_results_df(tool_box)
        self.assertEquals(len(results_df), 4)

        expected_list = list()
        expected_list.append({'fk': 642, 'score': 100, 'result': 100, 'numerator_id': 6, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 100, 'result': 100, 'numerator_id': 7, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 8, 'denominator_result': 1})
        expected_list.append({'fk': 642, 'score': 0, 'result': 0, 'numerator_id': 9, 'denominator_result': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_main_assortment_calculation_no_historical_products_in_session(self):
        lvl3_result = self.create_assortment_result_data_mock([1])
        tool_box = RISPARKWINEDEToolBox(self.data_provider_mock, self.output)
        tool_box.store_assortment = self.STORE_ASSORTMENT
        tool_box.common.write_to_db_result = MagicMock()
        tool_box.main_assortment_calculation(lvl3_result)

        results_df = self.build_results_df(tool_box)
        self.assertEquals(len(results_df), 14)
        self.assertEquals(len(results_df[results_df['fk'] == 642]), 0)

        expected_list = list()
        # distribution kpis
        expected_list.append({'fk': 4, 'score': 0, 'result': 0, 'numerator_id': 1, 'denominator_result': 1})
        expected_list.append({'fk': 4, 'score': 0, 'result': 0, 'numerator_id': 2, 'denominator_result': 1})

        expected_list.append({'fk': 5, 'score': 0, 'result': 0, 'numerator_id': 3, 'denominator_result': 1})
        expected_list.append({'fk': 5, 'score': 0, 'result': 0, 'numerator_id': 4, 'denominator_result': 1})
        expected_list.append({'fk': 5, 'score': 0, 'result': 0, 'numerator_id': 5, 'denominator_result': 1})

        expected_list.append({'fk': 10, 'score': 0, 'result': 0, 'numerator_id': 1, 'numerator_result': 0,
                              'denominator_id': 4, 'denominator_result': 2})
        expected_list.append({'fk': 11, 'score': 0, 'result': 0, 'numerator_id': 2, 'numerator_result': 0,
                              'denominator_id': 5, 'denominator_result': 3})
        expected_list.append({'fk': 3, 'score': 0, 'result': 0, 'numerator_result': 0,
                              'denominator_result': 5})

        #oos kpis
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 1})
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 2})

        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 3})
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 4})
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 5})

        expected_list.append({'fk': 2, 'score': 100, 'result': 100, 'numerator_result': 5,
                              'denominator_result': 5})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_main_assortment_calculation_historical_products_present_in_session(self):
        lvl3_result = self.create_assortment_result_data_mock([2])
        tool_box = RISPARKWINEDEToolBox(self.data_provider_mock, self.output)
        tool_box.store_assortment = self.STORE_ASSORTMENT
        tool_box.common.write_to_db_result = MagicMock()
        tool_box.main_assortment_calculation(lvl3_result)

        results_df = self.build_results_df(tool_box)
        results_df['score'] = results_df['score'].apply(lambda x: round(x, 2))
        results_df['result'] = results_df['result'].apply(lambda x: round(x, 2))
        self.assertEquals(len(results_df), 14)
        self.assertEquals(len(results_df[results_df['fk'] == 642]), 0)

        expected_list = list()
        # distribution kpis
        expected_list.append({'fk': 4, 'score': 0, 'result': 0, 'numerator_id': 1, 'denominator_result': 1,
                              'score_after_actions': 1})
        expected_list.append({'fk': 4, 'score': 100, 'result': 100, 'numerator_id': 2, 'denominator_result': 1,
                              'score_after_actions': 0})

        expected_list.append({'fk': 5, 'score': 100, 'result': 100, 'numerator_id': 3, 'denominator_result': 1,
                              'score_after_actions': 0})
        expected_list.append({'fk': 5, 'score': 100, 'result': 100, 'numerator_id': 4, 'denominator_result': 1,
                              'score_after_actions': 1})
        expected_list.append({'fk': 5, 'score': 0, 'result': 0, 'numerator_id': 5, 'denominator_result': 1,
                              'score_after_actions': 0})

        expected_list.append({'fk': 10, 'score': 0, 'result': 50, 'numerator_id': 1, 'numerator_result': 1,
                              'denominator_id': 4, 'denominator_result': 2})
        expected_list.append({'fk': 11, 'score': 0, 'result': round(2.0 / 3 * 100, 2),
                              'numerator_id': 2, 'numerator_result': 2, 'denominator_id': 5, 'denominator_result': 3})
        expected_list.append({'fk': 3, 'score': 3.0/5 * 100, 'result': 3.0/5 * 100, 'numerator_result': 3,
                              'denominator_result': 5})

        # oos kpis
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 1})
        expected_list.append({'fk': 1, 'result': 0, 'numerator_id': 2})

        expected_list.append({'fk': 1, 'result': 0, 'numerator_id': 3})
        expected_list.append({'fk': 1, 'result': 0, 'numerator_id': 4})
        expected_list.append({'fk': 1, 'result': 1, 'numerator_id': 5})

        expected_list.append({'fk': 2, 'score':  2.0/5 * 100, 'result':  2.0/5 * 100, 'numerator_result': 2,
                              'denominator_result': 5})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_assortment_calculation_does_not_add_wine_products_to_distribution_kpi(self):
        lvl3_result = self.create_assortment_result_data_mock([2])
        tool_box = RISPARKWINEDEToolBox(self.data_provider_mock, self.output)
        tool_box.store_assortment = self.STORE_ASSORTMENT
        tool_box.common.write_to_db_result = MagicMock()
        tool_box.main_calculation()

        results_df = self.build_results_df(tool_box)
        results_df['score'] = results_df['score'].apply(lambda x: round(x, 2))
        results_df['result'] = results_df['result'].apply(lambda x: round(x, 2))

        expected_list = list()
        expected_list.append({'fk': 3, 'score': 3.0 / 5 * 100, 'result': 3.0 / 5 * 100, 'numerator_result': 3,
                              'denominator_result': 5})
        expected_list.append({'fk': 2, 'score': 2.0 / 5 * 100, 'result': 2.0 / 5 * 100, 'numerator_result': 2,
                              'denominator_result': 5})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    @staticmethod
    def build_results_df(tool_box):
        results_list = [res[2] for res in tool_box.common.write_to_db_result.mock_calls]
        results_df = pd.DataFrame.from_records(results_list)
        return results_df

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
