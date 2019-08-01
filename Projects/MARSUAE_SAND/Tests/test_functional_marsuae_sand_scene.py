from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.MARSUAE_SAND.Tests.data_test_unit_marsuae_sand import DataTestUnitMarsuae
from Projects.MARSUAE_SAND.Utils.KPISceneToolBox import MARSUAE_SANDSceneToolBox
import pandas as pd
from pandas.util.testing import assert_frame_equal


__author__ = 'natalyak'


class TestMarsuaeSandScene(TestFunctionalCase):
    seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.MARSUAE_SAND.Utils.KPISceneToolBox'

    def set_up(self):
        super(TestMarsuaeSandScene, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.db_users_mock = self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        # self.probe_group_mock = self.mock_probe_group()
        # self.full_store_info = self.mock_store_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        # self.external_targets_mock = self.mock_kpi_external_targets_data()
        # self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        # self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        self.mock_all_products()
        # self.mock_all_templates()
        # self.mock_block()

    # def mock_store_data(self):
    #     store_data = self.mock_object('MARSUAE_SANDSceneToolBox.get_store_data_by_store_id')
    #     store_data.return_value = DataTestUnitMarsuae.store_data_sss_a
    #     return store_data.return_value

    # def mock_block(self):
    #     self.mock_object('Block')
    #

    def mock_probe_group(self, data):
        probe_group = self.mock_object('PEPSICOUKSceneToolBox.get_probe_group')
        probe_group.return_value = data.where(data.notnull(), None)
        return probe_group.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_db_users(self):
        return self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitMarsuae.kpi_static_data
        return static_kpi.return_value

    # def mock_kpi_external_targets_data(self):
    #     # print DataTestUnitPEPSICOUK.external_targets
    #     external_targets_df = pd.read_excel(DataTestUnitMarsuae.external_targets)
    #     external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
    #                                         path='Projects.PEPSICOUK.Utils.CommonToolBox')
    #     external_targets.return_value = external_targets_df
    #     return external_targets.return_value

    # def mock_kpi_result_value_table(self):
    #     kpi_result_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
    #                                         path='Projects.PEPSICOUK.Utils.CommonToolBox')
    #     kpi_result_value.return_value = DataTestUnitPEPSICOUK.kpi_results_values_table
    #     return kpi_result_value.return_value

    # def mock_kpi_score_value_table(self):
    #     kpi_score_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
    #                                        path='Projects.PEPSICOUK.Utils.CommonToolBox',)
    #     kpi_score_value.return_value = DataTestUnitPEPSICOUK.kpi_scores_values_table
    #     return kpi_score_value.return_value
    #
    # def mock_all_products(self):
    #     self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
    #                                                                  sheetname='all_products')

    def mock_all_products(self):
        self.data_provider_mock['all_products'] = DataTestUnitMarsuae.all_products_scene

    # def mock_all_templates(self):
    #     self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    # def mock_scene_item_facts(self, data):
    #     self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def test_calculate_price_generates_no_kpi_results_if_no_prices_in_scene(self):
        self.mock_match_product_in_scene(DataTestUnitMarsuae.scene_1_no_prices)
        scene_tb = MARSUAE_SANDSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_price()
        self.assertTrue(scene_tb.kpi_results.empty)

    def test_calculate_price_returns_max_result_and_only_for_products_with_prices(self):
        self.mock_match_product_in_scene(DataTestUnitMarsuae.scene_2)
        scene_tb = MARSUAE_SANDSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_price()
        expected_list = list()
        expected_list.append({'kpi_fk': 3004, 'numerator': 1, 'result': 4.5})
        expected_list.append({'kpi_fk': 3004, 'numerator': 3, 'result': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(scene_tb.kpi_results), 2)

    def test_calculate_price_returns_max_result_among_price_and_promo_price(self):
        self.mock_match_product_in_scene(DataTestUnitMarsuae.scene_3)
        scene_tb = MARSUAE_SANDSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_price()
        expected_list = list()
        expected_list.append({'kpi_fk': 3004, 'numerator': 1, 'result': 5})
        expected_list.append({'kpi_fk': 3004, 'numerator': 3, 'result': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))
        self.assertEquals(len(scene_tb.kpi_results), 2)

    @staticmethod
    def check_kpi_results(kpi_results_df, expected_results_dict):
        column = []
        expression = []
        condition = []
        for key, value in expected_results_dict.items():
            column.append(key)
            expression.append('==')
            condition.append(value)
        query = ' & '.join('{} {} {}'.format(i, j, k) for i, j, k in zip(column, expression, condition))
        filtered_df = kpi_results_df.query(query)
        return len(filtered_df)