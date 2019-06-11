from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from Trax.Data.Testing.SeedNew import Seeder
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from mock import MagicMock
from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Projects.PEPSICOUK.Tests.data_test_unit_pepsicouk import DataTestUnitPEPSICOUK, DataScores
from Trax.Algo.Calculations.Core.DataProvider import Output
from mock import patch
import os
import pandas as pd
from pandas.util.testing import assert_frame_equal
from Projects.PEPSICOUK.KPIs.Session.HeroAvailabilitySKU import HeroAvailabilitySkuKpi
from Projects.PEPSICOUK.KPIs.Session.HeroAvailability import HeroAvailabilityKpi

__author__ = 'natalya'


def get_exclusion_template_df_all_tests():
    template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
    return template_df


class Test_PEPSICOUK(MockingTestCase):
    # template_df_mock = get_exclusion_template_df_all_tests()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.KPIs.Util'

    def set_up(self):
        super(Test_PEPSICOUK, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        self.mock_various_project_connectors()
        self.project_connector_mock = self.mock_project_connector()

        self.ps_dataprovider_project_connector_mock = self.mock_ps_data_provider_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        self.full_store_data_mock = self.mock_store_data()

        self.probe_groups_mock = self.mock_probe_groups()
        self.custom_entity_data_mock = self.mock_custom_entity_data()
        self.on_display_products_mock = self.mock_on_display_products()

        self.exclusion_template_mock = self.mock_template_data()
        self.store_policy_template_mock = self.mock_store_policy_exclusion_template_data()
        self.output = MagicMock()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        # self.assortment_mock = self.mock_assortment_object()
        self.lvl3_ass_result_mock = self.mock_lvl3_ass_result()
        self.lvl3_ass_base_mock = self.mock_lvl2_ass_base_df()
        self.mock_all_products()
        self.mock_all_templates()
        self.mock_position_graph()

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKCommonToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_lvl3_ass_result(self):
        ass_res = self.mock_object('Assortment.calculate_lvl3_assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = DataTestUnitPEPSICOUK.test_case_1_ass_result
        return ass_res.return_value

    def mock_lvl2_ass_base_df(self):
        ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
                                       path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = DataTestUnitPEPSICOUK.test_case_1_ass_base_extended
        return ass_res.return_value

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
                                                                     sheetname='all_products')

    def mock_all_templates(self):
        self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    def mock_scene_info(self, data):
        self.data_provider_data_mock['scenes_info'] = data.where(data.notnull(), None)

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider'), self.mock_object('DbUsers')
        # self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        # self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        # self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')

        # KPIUtils_v2 / DB / PsProjectConnector
        # self.mock_object('DbUsers', path='KPIUtils_v2.DB.PsProjectConnector'), self.mock_object('DbUsers')

    def mock_on_display_products(self):
        on_display_products = self.mock_object('PEPSICOUKCommonToolBox.get_on_display_products',
                                               path='Projects.PEPSICOUK.Utils.CommonToolBox')
        on_display_products.return_value = DataTestUnitPEPSICOUK.on_display_products
        return on_display_products.return_value

    def mock_assortment_object(self):
        return self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_external_targets_data(self):
        external_targets_df = pd.read_excel(DataTestUnitPEPSICOUK.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_probe_groups(self):
        probe_groups_df = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='stitch_groups')
        probe_groups = self.mock_object('PepsicoUtil.get_probe_group')
        probe_groups.return_value = probe_groups_df
        return probe_groups.return_value

    def mock_probe_group_for_particular_test_case(self, data):
        probe_group = self.mock_object('PEPSICOUKSceneToolBox.get_probe_group')
        probe_group.return_value = data.where(data.notnull(), None)
        return probe_group.return_value

    def mock_custom_entity_data(self):
        custom_entities = self.mock_object('PEPSICOUKCommonToolBox.get_custom_entity_data',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox')
        custom_entities.return_value = DataTestUnitPEPSICOUK.custom_entity
        return custom_entities.return_value

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitPEPSICOUK.kpi_static_data
        return static_kpi.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_ps_data_provider_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    # def mock_static_kpi(self):
    #     static_kpi = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_static_data',
    #                                   value=DataTestUnitPEPSICOUK.kpi_static_data,
    #                                   path='Projects.PEPSICOUK.Utils.CommonToolBox')
    #     # static_kpi.return_value = DataTestUnitCCBZA_SAND.static_data
    #     return static_kpi.return_value

    # def mock_store_data(self):
    #     store_data = self.mock_object('PEPSICOUKToolBox.get_store_data_by_store_id')
    #     store_data.return_value = DataTestUnitPEPSICOUK.store_data
    #     return store_data.return_value

    def mock_store_policy_exclusion_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path, sheet_name='store_policy')
        template_df = template_df.fillna('ALL')
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_store_policy_data_for_exclusion_template',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value

    def mock_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path, sheet_name='exclusion_rules')
        template_df = template_df.fillna('')
        # template_df = Test_PEPSICOUK.template_df_mock
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value

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

    def mock_scene_kpi_results(self, data):
        scene_results = self.mock_object('PsDataProvider.get_scene_results',
                                         path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        scene_results.return_value = data
        return

    def test_util_remains_singleton_for_various_kpis(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
        availability = HeroAvailabilityKpi(self.data_provider_mock, config_params={}, dependencies_data=pd.DataFrame())
        self.assertTrue(availability_sku.util is availability.util)

    def test_get_available_hero_sku_list_retrieves_only_skus_in_store(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        util = PepsicoUtil(self.output, self.data_provider_mock)
        availability_sku = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
        availability_sku.calculate()
        hero_dependency_df = pd.DataFrame(availability_sku.kpi_results)
        hero_dependency_df['kpi_type'] = util.HERO_SKU_AVAILABILITY_SKU
        hero_list = util.get_available_hero_sku_list(hero_dependency_df)
        self.assertItemsEqual(hero_list, [1, 2])

    def test_sos_vs_target_targets_property(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        util = PepsicoUtil(self.output, self.data_provider_mock)
        self.assertItemsEqual(util.sos_vs_target_targets['pk'].values.tolist(), [21, 22, 23, 24, 25])
        expected_list = list()
        expected_list.append({'pk': 21, 'numerator_id': 2, 'denominator_id': 8})
        expected_list.append({'pk': 22, 'numerator_id': 2, 'denominator_id': 11})
        expected_list.append({'pk': 23, 'numerator_id': 155, 'denominator_id': 2})
        expected_list.append({'pk': 24, 'numerator_id': 10, 'denominator_id': 2})
        expected_list.append({'pk': 25, 'numerator_id': 1515, 'denominator_id': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_df_values(util.sos_vs_target_targets, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    @staticmethod
    def check_df_values(results_df, expected_results_dict):
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

    # def test_whatever(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     tool_box = PepsicoUtil(self.output, self.data_provider_mock)
    #     print tool_box.exclusion_template
    #     print tool_box.probe_groups
    #     print tool_box.lvl3_ass_result[['product_fk', 'in_store']]
    #     av = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
    #     av.calculate()
    #     print av.util
    #     aval_res = pd.DataFrame(av.kpi_results)
    #     print aval_res[['numerator_id', 'numerator_result']]
    #     aval_res['kpi_type'] = tool_box.HERO_SKU_AVAILABILITY_SKU
    #     av_all = HeroAvailabilityKpi(self.data_provider_mock, config_params={}, dependencies_data=aval_res)
    #     av_all.calculate()
    #     print av_all.util
    #     print pd.DataFrame(av_all.kpi_results)[['numerator_id', 'result']]
    #     print tool_box.kpi_results_check
    #     print av.util is av_all.util
    #     print tool_box.scene_kpi_results
    #     print tool_box.scene_info
