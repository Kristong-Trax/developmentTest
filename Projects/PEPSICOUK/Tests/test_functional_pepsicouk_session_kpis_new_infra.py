# from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
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
from Projects.PEPSICOUK.KPIs.Session.BrandFullBay import BrandFullBayKpi
from Projects.PEPSICOUK.KPIs.Session.HeroSkuPrice import HeroSkuPriceKpi
from Projects.PEPSICOUK.KPIs.Session.HeroSkuPromoPrice import HeroSkuPromoPriceKpi
from Projects.PEPSICOUK.KPIs.Session.HeroSkuStackingBySequenceNumber import HeroSkuStackingBySequenceNumberKpi
from Projects.PEPSICOUK.KPIs.Session.LinearBrandVsBrandIndex import LinearBrandVsBrandIndexKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetHeroSKU import SosVsTargetHeroSkuKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetParent import SosVsTargetParentKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetSubSegment import SosVsTargetSubSegmentKpi
from Projects.PEPSICOUK.KPIs.Session.ShelfPlacementHeroSkusParent import ShelfPlacementHeroSkusParentKpi
from Projects.PEPSICOUK.KPIs.Session.ShelfPlacementHeroSkus import ShelfPlacementHeroSkusKpi
from Projects.PEPSICOUK.KPIs.Session.HeroPlacement import HeroPlacementKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetBrand import SosVsTargetBrandKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetSubBrand import SosVsTargetSubBrandKpi
from Projects.PEPSICOUK.KPIs.Session.SosVsTargetSegment import SosVsTargetSegmentKpi

__author__ = 'natalya'


def get_exclusion_template_df_all_tests():
    template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
    return template_df


class Test_PEPSICOUK(TestFunctionalCase):
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

    def get_hero_sku_availability_result(self):
        availability_sku = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
        availability_sku.calculate()
        availabiliity_sku_res = pd.DataFrame(availability_sku.kpi_results)
        availabiliity_sku_res['kpi_type'] = PepsicoUtil.HERO_SKU_AVAILABILITY_SKU
        return availabiliity_sku_res

    def test_hero_availability_sku_kpi(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
        availability_sku.calculate()
        availabiliity_res = pd.DataFrame(availability_sku.kpi_results)
        expected_list = list()
        expected_list.append({'numerator_id': 1, 'numerator_result': 1, 'denominator_result': 1, 'result': 4, 'score': 100})
        expected_list.append({'numerator_id': 2, 'numerator_result': 1, 'denominator_result': 1, 'result': 4, 'score': 100})
        expected_list.append({'numerator_id': 5, 'numerator_result': 0, 'denominator_result': 1, 'result': 5, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(availabiliity_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_hero_availability_kpi(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availabiliity_sku_res = self.get_hero_sku_availability_result()
        availablity = HeroAvailabilityKpi(self.data_provider_mock, config_params={},
                                          dependencies_data=availabiliity_sku_res)
        availablity.calculate()
        availabiliity_res = pd.DataFrame(availablity.kpi_results)
        availabiliity_res['result'] = availabiliity_res['result'].apply(lambda x: round(x, 2))
        expected_list = list()
        expected_list.append({'numerator_id': 2, 'numerator_result': 2, 'denominator_result': 3,
                              'result': round(2/float(3) * 100, 2), 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(availabiliity_res, expected_result) == 1)
        self.assertTrue(all(test_result_list))

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
            test_result_list.append(self.check_kpi_results(util.sos_vs_target_targets, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_brand_full_bay_calculates_kpi_depending_on_parameter(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        brand_full_bay = BrandFullBayKpi(self.data_provider_mock, config_params={'ratio': '1'})
        brand_full_bay.calculate()
        kpi_results = pd.DataFrame(brand_full_bay.kpi_results)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 316, 'numerator_id': 167, 'score': 0})
        expected_list.append({'kpi_level_2_fk': 316, 'numerator_id': 168, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        brand_full_bay_90 = BrandFullBayKpi(self.data_provider_mock, config_params={'ratio': '0.9'})
        brand_full_bay_90.calculate()
        kpi_results = pd.DataFrame(brand_full_bay_90.kpi_results)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 327, 'numerator_id': 167, 'score': 1})
        expected_list.append({'kpi_level_2_fk': 327, 'numerator_id': 168, 'score': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_hero_sku_price_kpi_calculates_price_only_for_available_products(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()
        price = HeroSkuPriceKpi(self.data_provider_mock, config_params={}, dependencies_data=availability_sku_res)
        price.calculate()
        kpi_results = pd.DataFrame(price.kpi_results)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 317, 'numerator_id': 1, 'result': 9})
        expected_list.append({'kpi_level_2_fk': 317, 'numerator_id': 2, 'result': -1})
        self.assertEquals(len(kpi_results), 2)
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_hero_sku_price_kpi_writes_no_results_if_no_available_products(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()
        availability_sku_res['numerator_result'] = 0
        price = HeroSkuPriceKpi(self.data_provider_mock, config_params={}, dependencies_data=availability_sku_res)
        price.calculate()
        kpi_results = pd.DataFrame(price.kpi_results)
        self.assertEquals(len(kpi_results), 0)

    def test_hero_sku_promo_price_kpi_calculates_price_only_for_available_products(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()
        price = HeroSkuPromoPriceKpi(self.data_provider_mock, config_params={}, dependencies_data=availability_sku_res)
        price.calculate()
        kpi_results = pd.DataFrame(price.kpi_results)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 318, 'numerator_id': 1, 'result': 4})
        expected_list.append({'kpi_level_2_fk': 318, 'numerator_id': 2, 'result': 5})
        self.assertEquals(len(kpi_results), 2)
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_hero_stacking_by_sequence_number(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()
        stacking = HeroSkuStackingBySequenceNumberKpi(self.data_provider_mock, config_params={},
                                                      dependencies_data=availability_sku_res)
        stacking.calculate()
        kpi_results = pd.DataFrame(stacking.kpi_results)
        expected_skus_in_results = [1, 2]
        self.assertItemsEqual(kpi_results['numerator_id'].unique().tolist(), expected_skus_in_results)
        self.assertEquals(len(kpi_results), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 315, 'numerator_id': 1, 'result': 4, 'score': 1})
        expected_list.append({'kpi_level_2_fk': 315, 'numerator_id': 2, 'result': 4, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_linear_brand_vs_brand_index(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        brand_vs_brand = LinearBrandVsBrandIndexKpi(self.data_provider_mock,
                                                    config_params={"kpi_type": "Doritos Greater Linear space vs Pringles"})
        brand_vs_brand.calculate()
        kpi_results = pd.DataFrame(brand_vs_brand.kpi_results)
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        self.assertEquals(len(kpi_results), 1)
        expected_list = list()
        # expected_list.append({'kpi_fk': 301, 'numerator': 194, 'denominator': 183, 'score': 0})
        expected_list.append({'kpi_level_2_fk': 302, 'numerator_id': 136, 'denominator_id': 189, 'numerator_result': 60,
                              'denominator_result': 135, 'score': round((float(60) / 195) / (float(135) / 195), 5),
                              'result': round((float(60) / 195) / (float(135) / 195), 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        brand_vs_brand_2 = LinearBrandVsBrandIndexKpi(self.data_provider_mock,
                                                      config_params={"kpi_type": "Sensations Greater Linear Space vs Kettle"})
        brand_vs_brand_2.calculate()
        kpi_results = pd.DataFrame(brand_vs_brand_2.kpi_results)
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        print kpi_results[['kpi_level_2_fk', 'denominator_id', 'denominator_result']]
        self.assertEquals(len(kpi_results), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 301, 'numerator_id': 194, 'denominator_id': 183, 'numerator_result': 0,
                              'denominator_result': 0, 'score': 0,
                              'result': 0})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target_hero_sku_index_and_parent(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()
        sos_vs_target_hero = SosVsTargetHeroSkuKpi(self.data_provider_mock,
                                                   config_params={"kpi_type": "Hero SKU Space to Sales Index"},
                                                   dependencies_data=availability_sku_res)
        sos_vs_target_hero.calculate()
        kpi_results = pd.DataFrame(sos_vs_target_hero.kpi_results)
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        self.assertEquals(len(kpi_results), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 287, 'numerator_id': 1, 'denominator_id': 2, 'numerator_result': 120,
                              'denominator_result': 435, 'score': 0,
                              'result': round((float(120) / 435) * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 287, 'numerator_id': 2, 'denominator_id': 2, 'numerator_result': 60,
                              'denominator_result': 435, 'score': 0,
                              'result': round((float(60) / 435) * 100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # check parent
        sos_parent = SosVsTargetParentKpi(self.data_provider_mock, config_params={}, dependencies_data=kpi_results)
        sos_parent.calculate()
        sos_parent_results = pd.DataFrame(sos_parent.kpi_results)
        self.assertEquals(len(sos_parent_results), 1)
        expected_list = list()
        expected_list.append({'numerator_id': 2, 'score': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(sos_parent_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target_sub_segment_index_and_parent(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        sos_vs_target_sub_segm = SosVsTargetSubSegmentKpi(self.data_provider_mock,
                                                          config_params={"kpi_type": "PepsiCo Sub Segment Space to Sales Index"})
        sos_vs_target_sub_segm.calculate()
        kpi_results = pd.DataFrame(sos_vs_target_sub_segm.kpi_results)
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        print kpi_results[['numerator_id', 'numerator_result', 'target', 'result', 'score']]
        self.assertEquals(len(kpi_results), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 296, 'numerator_id': 2, 'denominator_id': 11, 'numerator_result': 135,
                              'denominator_result': 255, 'score': round((float(135) / 255)/0.9, 5),
                              'result': round((float(135) / 255) * 100, 5), 'target': 90})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # check parent
        sos_parent = SosVsTargetParentKpi(self.data_provider_mock, config_params={}, dependencies_data=kpi_results)
        sos_parent.calculate()
        sos_parent_results = pd.DataFrame(sos_parent.kpi_results)
        self.assertEquals(len(sos_parent_results), 1)
        expected_list = list()
        expected_list.append({'numerator_id': 2, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(sos_parent_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target_brand_index_and_parent(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        sos_vs_target_brand = SosVsTargetBrandKpi(self.data_provider_mock,
                                                  config_params={"kpi_type": "Brand Space to Sales Index"})
        sos_vs_target_brand.calculate()
        kpi_results = pd.DataFrame(sos_vs_target_brand.kpi_results)
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        print kpi_results[['numerator_id', 'numerator_result', 'denominator_result', 'result']]
        self.assertEquals(len(kpi_results), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 293, 'numerator_id': 136, 'denominator_id': 2, 'numerator_result': 180,
                              'denominator_result': 300, 'score': 0,
                              'result': 60})
        expected_list.append({'kpi_level_2_fk': 293, 'numerator_id': 138, 'denominator_id': 2, 'numerator_result': 120,
                              'denominator_result': 300, 'score': 0,
                              'result': 40})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # check parent
        sos_parent = SosVsTargetParentKpi(self.data_provider_mock, config_params={}, dependencies_data=kpi_results)
        sos_parent.calculate()
        sos_parent_results = pd.DataFrame(sos_parent.kpi_results)
        self.assertEquals(len(sos_parent_results), 1)
        expected_list = list()
        expected_list.append({'numerator_id': 2, 'score': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(sos_parent_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target_sub_brand_index_and_parent(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        sos_vs_target_sub_brand = SosVsTargetSubBrandKpi(self.data_provider_mock,
                                                     config_params={"kpi_type": "Sub Brand Space to Sales Index"})
        sos_vs_target_sub_brand.calculate()
        kpi_results = pd.DataFrame(sos_vs_target_sub_brand.kpi_results)
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        kpi_results['score'] = kpi_results['score'].apply(lambda x: round(x, 5))
        print kpi_results[['numerator_id', 'numerator_result', 'denominator_result', 'result', 'target']]
        self.assertEquals(len(kpi_results), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 294, 'numerator_id': 10, 'denominator_id': 2, 'numerator_result': 120,
                              'denominator_result': 435, 'result': round((float(120) / 435) * 100, 5), 'target': 2.0,
                              'score': round((float(120) / 435)/0.02, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # check parent
        sos_parent = SosVsTargetParentKpi(self.data_provider_mock, config_params={}, dependencies_data=kpi_results)
        sos_parent.calculate()
        sos_parent_results = pd.DataFrame(sos_parent.kpi_results)
        self.assertEquals(len(sos_parent_results), 1)
        expected_list = list()
        expected_list.append({'numerator_id': 2, 'score': 1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(sos_parent_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_sos_vs_target_segment_index_and_parent_when_no_child_results(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        sos_vs_target_segment = SosVsTargetSegmentKpi(self.data_provider_mock,
                                                      config_params={"kpi_type": "PepsiCo Segment Space to Sales Index"})
        sos_vs_target_segment.calculate()
        kpi_results = pd.DataFrame(sos_vs_target_segment.kpi_results)
        self.assertTrue(kpi_results.empty)

        # check parent
        sos_parent = SosVsTargetParentKpi(self.data_provider_mock, config_params={}, dependencies_data=kpi_results)
        sos_parent.calculate()
        sos_parent_results = pd.DataFrame(sos_parent.kpi_results)
        self.assertEquals(len(sos_parent_results), 0)

    def test_hero_shelf_placement_kpi_flow(self):
        self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='scif'))
        self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheet_name='matches'))
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        availability_sku_res = self.get_hero_sku_availability_result()

        # hero top
        hero_top = ShelfPlacementHeroSkusKpi(self.data_provider_mock,
                                             config_params={"level": "Placement by shelf numbers_Top"},
                                             dependencies_data=availability_sku_res)
        hero_top.calculate()
        hero_top_results = pd.DataFrame(hero_top.kpi_results)
        hero_top_results['result'] = hero_top_results['result'].apply(lambda x: round(x, 5))
        hero_top_results['score'] = hero_top_results['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(hero_top_results), 1)
        expected_list = []
        expected_list.append({'kpi_level_2_fk': 311, 'numerator_id': 1, 'denominator_id': 1,
                              'numerator_result': 7, 'denominator_result': 12, 'score': round(7.0 * 100 / 12, 5),
                              'result': round(7.0 * 100 / 12, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(hero_top_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # hero eye
        hero_eye = ShelfPlacementHeroSkusKpi(self.data_provider_mock,
                                             config_params={"level": "Placement by shelf numbers_Eye"},
                                             dependencies_data=availability_sku_res)
        hero_eye.calculate()
        hero_eye_results = pd.DataFrame(hero_eye.kpi_results)
        hero_eye_results['result'] = hero_eye_results['result'].apply(lambda x: round(x, 5))
        hero_eye_results['score'] = hero_eye_results['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(hero_eye_results), 2)
        expected_list = []
        expected_list.append({'kpi_level_2_fk': 312, 'numerator_id': 1, 'denominator_id': 1,
                              'numerator_result': 3, 'denominator_result': 12, 'score': round(3.0 * 100 / 12, 5),
                              'result': round(3.0 * 100 / 12, 5)})
        expected_list.append({'kpi_level_2_fk': 312, 'numerator_id': 2, 'denominator_id': 2,
                              'numerator_result': 8, 'denominator_result': 12, 'score': round(8.0 * 100 / 12, 5),
                              'result': round(8.0 * 100 / 12, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(hero_eye_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # hero_middle
        hero_mid = ShelfPlacementHeroSkusKpi(self.data_provider_mock,
                                             config_params={"level": "Placement by shelf numbers_Middle"},
                                             dependencies_data=availability_sku_res)
        hero_mid.calculate()
        hero_mid_results = pd.DataFrame(hero_mid.kpi_results)
        hero_mid_results['result'] = hero_mid_results['result'].apply(lambda x: round(x, 5))
        hero_mid_results['score'] = hero_mid_results['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(hero_mid_results), 1)
        expected_list = []
        expected_list.append({'kpi_level_2_fk': 313, 'numerator_id': 2, 'denominator_id': 2,
                              'numerator_result': 2, 'denominator_result': 12, 'score': round(2.0 * 100 / 12, 5),
                              'result': round(2.0 * 100 / 12, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(hero_mid_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # hero_bottom
        hero_bottom = ShelfPlacementHeroSkusKpi(self.data_provider_mock,
                                                config_params={"level": "Placement by shelf numbers_Bottom"},
                                                dependencies_data=availability_sku_res)
        hero_bottom.calculate()
        hero_bottom_results = pd.DataFrame(hero_bottom.kpi_results)
        hero_bottom_results['result'] = hero_bottom_results['result'].apply(lambda x: round(x, 5))
        hero_bottom_results['score'] = hero_bottom_results['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(hero_bottom_results), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 314, 'numerator_id': 1, 'denominator_id': 1,
                              'numerator_result': 2, 'denominator_result': 12, 'score': round(2.0 * 100 / 12, 5),
                              'result': round(2.0 * 100 / 12, 5)})
        expected_list.append({'kpi_level_2_fk': 314, 'numerator_id': 2, 'denominator_id': 2,
                              'numerator_result': 2, 'denominator_result': 12, 'score': round(2.0 * 100 / 12, 5),
                              'result': round(2.0 * 100 / 12, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(hero_bottom_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # hero placement by sku
        hero_all_levels = hero_top_results.copy()
        hero_all_levels = hero_all_levels.append(hero_eye_results)
        hero_all_levels = hero_all_levels.append(hero_bottom_results)
        hero_all_levels = hero_all_levels.append(hero_mid_results)
        hero_placement = ShelfPlacementHeroSkusParentKpi(self.data_provider_mock, config_params={},
                                                         dependencies_data=hero_all_levels)
        hero_placement.calculate()
        hero_results = pd.DataFrame(hero_placement.kpi_results)
        hero_results['result'] = hero_results['result'].apply(lambda x: round(x, 1))
        hero_results['score'] = hero_results['score'].apply(lambda x: round(x, 1))
        self.assertEquals(len(hero_results), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 310, 'numerator_id': 1,
                              'score': 1 * 100, 'result': 1 * 100})
        expected_list.append({'kpi_level_2_fk': 310, 'numerator_id': 2, 'score':  1 * 100,
                              'result':  1 * 100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(hero_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # top hero kpi
        top_hero = HeroPlacementKpi(self.data_provider_mock, config_params={},
                                    dependencies_data=hero_results)
        top_hero.calculate()
        top_results = pd.DataFrame(top_hero.kpi_results)
        self.assertEquals(len(top_results), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 309, 'numerator_id': 2, 'score': 2, 'result': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(top_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    # def test_whatever(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     tool_box = PepsicoUtil(self.output, self.data_provider_mock)
    #     # print tool_box.exclusion_template
    #     # print tool_box.probe_groups
    #     # print tool_box.lvl3_ass_result[['product_fk', 'in_store']]
    #     av = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
    #     av.calculate()
    #     print av.util
    #     aval_res = pd.DataFrame(av.kpi_results)
    #     # print aval_res[['numerator_id', 'numerator_result']]
    #     aval_res['kpi_type'] = tool_box.HERO_SKU_AVAILABILITY_SKU
    #     av_all = HeroAvailabilityKpi(self.data_provider_mock, config_params={}, dependencies_data=aval_res)
    #     av_all.calculate()
    #     print av_all.util
    #     # print pd.DataFrame(av_all.kpi_results)[['numerator_id', 'result']]
    #     # print tool_box.kpi_results_check
    #     print av.util is av_all.util
    #     #     print tool_box.scene_kpi_results
    #     #     print tool_box.scene_info

    @staticmethod
    def check_kpi_results(results_df, expected_results_dict):
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