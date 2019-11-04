from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.NESTLEIL.Tests.data_test_unit_nestleil import DataTestUnitNestleil
from Projects.NESTLEIL.Utils.KPIToolBox import NESTLEILToolBox
import pandas as pd


__author__ = 'natalyak'


def get_test_case_template_all_tests():
    test_case_xls_object = pd.ExcelFile(DataTestUnitNestleil.test_case_1)
    return test_case_xls_object


def get_scif_matches_stich_groups(xls_file):
    scif_test_case = pd.read_excel(xls_file, sheetname='scif')
    matches_test_case = pd.read_excel(xls_file, sheetname='matches')
    return scif_test_case, matches_test_case


class TestNestleil(TestFunctionalCase):

    TEST_CASE = get_test_case_template_all_tests()
    SCIF, MATCHES = get_scif_matches_stich_groups(TEST_CASE)

    @property
    def import_path(self):
        return 'Projects.NESTLEIL.Utils.KPIToolBox'

    def set_up(self):
        super(TestNestleil, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        # self.mock_lvl3_ass()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.mock_all_products()
        self.mock_various_project_connectors()
        self.mock_position_graph()

    # def mock_lvl3_ass_result(self):
    #     ass_res = self.mock_object('Assortment.calculate_lvl3_assortment',
    #                                path='KPIUtils_v2.Calculations.AssortmentCalculations')
    #     ass_res.return_value = DataTestUnitMarsuae.test_case_1_ass_result
    #     return ass_res.return_value

    def mock_position_graph_block(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.BlockCalculations_v2')

    def mock_lvl3_ass(self, data):
        ass_res = self.mock_object('Assortment.calculate_lvl3_assortment',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = data
        return ass_res.return_value

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('PsDataProvider.get_result_values')
        kpi_result_value.return_value = DataTestUnitNestleil.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2')
        self.mock_object('DbUsers', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitNestleil.kpi_static_data
        return static_kpi.return_value

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(self.TEST_CASE,
                                                                     sheetname='all_products')

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

    def test_calculate_assortment_hierarchy_and_results(self):
        self.mock_lvl3_ass(DataTestUnitNestleil.assortment_res)
        tool_box = NESTLEILToolBox(self.data_provider_mock, self.output)
        tool_box.common_v2.write_to_db_result = MagicMock()
        tool_box.own_manufacturer_fk = 3
        tool_box._calculate_assortment()

        calls = tool_box.common_v2.write_to_db_result.mock_calls
        calls_list = [call[2] for call in calls]
        calls_df = pd.DataFrame.from_records(calls_list)

        #snacks
        distr_snacks_child_res, distr_snacks_par = self.retrieve_child_results_by_parent(2004, calls_df,
                                                                                         tool_box.own_manufacturer_fk)
        self.assertEquals(len(distr_snacks_child_res), 3)
        expected_results = list()
        expected_results.append({'fk': 2006, 'numerator_id': 152, 'score': 0, 'result': 1, 'denominator_result': 1,
                                 'numerator_result': 0})
        expected_results.append({'fk': 2006, 'numerator_id': 153, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        expected_results.append({'fk': 2006, 'numerator_id': 157, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(distr_snacks_child_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(distr_snacks_par), 1)
        expected_parent_res = {'fk': 2004, 'numerator_id': 3, 'score': 66.67, 'result': 66.67, 'denominator_result': 3,
                               'numerator_result': 2}
        self.assertEquals(self.check_results(distr_snacks_par, expected_parent_res), 1)

        oos_snacks_child_res, oos_snacks_par = self.retrieve_child_results_by_parent(2008, calls_df,
                                                                                     tool_box.own_manufacturer_fk)
        self.assertEquals(len(oos_snacks_child_res), 3)
        expected_results = list()
        expected_results.append({'fk': 2010, 'numerator_id': 152, 'score': 0, 'result': 1, 'denominator_result': 1,
                                 'numerator_result': 0})
        expected_results.append({'fk': 2010, 'numerator_id': 153, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        expected_results.append({'fk': 2010, 'numerator_id': 157, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(oos_snacks_child_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(oos_snacks_par), 1)
        expected_parent_res = {'fk': 2008, 'numerator_id': 3, 'score': 33.33, 'result': 33.33, 'denominator_result': 3,
                               'numerator_result': 1}
        self.assertEquals(self.check_results(oos_snacks_par, expected_parent_res), 1)

        #sabra
        distr_sabra_child_res, distr_sabra_par = self.retrieve_child_results_by_parent(2005, calls_df,
                                                                                       tool_box.own_manufacturer_fk)
        self.assertEquals(len(distr_sabra_child_res), 2)
        expected_results = list()
        expected_results.append({'fk': 2007, 'numerator_id': 253, 'score': 0, 'result': 1, 'denominator_result': 1,
                                 'numerator_result': 0})
        expected_results.append({'fk': 2007, 'numerator_id': 255, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(distr_sabra_child_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(distr_sabra_par), 1)
        expected_parent_res = {'fk': 2005, 'numerator_id': 3, 'score': 50, 'result': 50, 'denominator_result': 2,
                               'numerator_result': 1}
        self.assertEquals(self.check_results(distr_sabra_par, expected_parent_res), 1)

        oos_sabra_child_res, oos_sabra_par = self.retrieve_child_results_by_parent(2009, calls_df,
                                                                                   tool_box.own_manufacturer_fk)
        self.assertEquals(len(oos_sabra_child_res), 2)
        expected_results = list()
        expected_results.append({'fk': 2011, 'numerator_id': 253, 'score': 0, 'result': 1, 'denominator_result': 1,
                                 'numerator_result': 0})
        expected_results.append({'fk': 2011, 'numerator_id': 255, 'score': 100, 'result': 2, 'denominator_result': 1,
                                 'numerator_result': 1})
        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(oos_sabra_child_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(oos_sabra_par), 1)
        expected_parent_res = {'fk': 2009, 'numerator_id': 3, 'score': 50, 'result': 50, 'denominator_result': 2,
                               'numerator_result': 1}
        self.assertEquals(self.check_results(oos_sabra_par, expected_parent_res), 1)

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    @staticmethod
    def retrieve_child_results_by_parent(parent_kpi, results_df, manuf):
        identifier_parent = (parent_kpi, manuf)
        child_results = results_df[results_df['identifier_parent'] == identifier_parent]
        parent_result = results_df[results_df['identifier_result'] == identifier_parent]
        return child_results, parent_result

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