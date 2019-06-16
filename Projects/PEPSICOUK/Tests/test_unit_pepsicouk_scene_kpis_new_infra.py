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
import numpy as np
from pandas.util.testing import assert_frame_equal
from Projects.PEPSICOUK.KPIs.Scene.NumberOfShelves import NumberOfShelvesKpi
from Projects.PEPSICOUK.KPIs.Scene.NumberOfBays import NumberOfBaysKpi
from Projects.PEPSICOUK.KPIs.Scene.NumberOfFacings import NumberOfFacingsKpi
from Projects.PEPSICOUK.KPIs.Scene.LinearSpace import LinearSpaceKpi
from Projects.PEPSICOUK.KPIs.Scene.ShelfPlacementVertical import ShelfPlacementVerticalKpi
from Projects.PEPSICOUK.KPIs.Scene.ShelfPlacementHorizontal import ShelfPlacementHorizontalKpi
from Projects.PEPSICOUK.KPIs.Scene.ProductBlocking import ProductBlockingKpi

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
        self.mock_position_graph_block()
        self.mock_position_graph_adjacency()

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKCommonToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_position_graph_block(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.BlockCalculations')

    def mock_position_graph_adjacency(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AdjacencyCalculations')

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

    def mock_block_results(self, data):
        block_results = self.mock_object('Block.network_x_block_together',
                                         path='KPIUtils_v2.Calculations.BlockCalculations')
        block_results.side_effect = data
        return

    def test_adjacency(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AdjacencyCalculations')


    def test_block_together_vertical_and_horizontal(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        self.mock_block_results([DataTestUnitPEPSICOUK.block_results, DataTestUnitPEPSICOUK.block_results_2])
        block = ProductBlockingKpi(self.data_provider_mock)
        block.calculate()
        kpi_result = pd.DataFrame(block.kpi_results)
        self.assertEquals(len(kpi_result), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 319, 'numerator_id': 165, 'numerator_result': 92, 'result': 4,
                              'score': 7, 'target': 90})
        expected_list.append(
            {'kpi_level_2_fk': 319, 'numerator_id': 166, 'numerator_result': 95, 'result': 4, 'score': 6, 'target': 90})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        expected_util_result = list()
        expected_util_result.append({'Group Name': 'Pringles_FTT_Tubes', 'Score': 1})
        expected_util_result.append({'Group Name': 'Hula Hoops_LMP_Snacks', 'Score': 1})
        test_result_list = list()
        for exp_res in expected_util_result:
            test_result_list.append(len(block.util.block_results[(block.util.block_results['Group Name']==exp_res['Group Name']) &
                                                                 (block.util.block_results['Score'] == exp_res[
                                                                 'Score'])]) == 1)
        self.assertTrue(all(test_result_list))

    def test_block_together_blocks_fail(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        self.mock_block_results([DataTestUnitPEPSICOUK.block_results_empty, DataTestUnitPEPSICOUK.block_results_failed])
        block = ProductBlockingKpi(self.data_provider_mock)
        block.calculate()
        kpi_result = pd.DataFrame(block.kpi_results)
        self.assertEquals(len(kpi_result), 2)
        expected_list = list()
        expected_list.append(
            {'kpi_level_2_fk': 319, 'numerator_id': 165, 'numerator_result': 0, 'result': 5, 'score': 0, 'target': 90})
        expected_list.append(
            {'kpi_level_2_fk': 319, 'numerator_id': 166, 'numerator_result': 60, 'result': 5, 'score': 0, 'target': 90})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        expected_util_result = list()
        expected_util_result.append({'Group Name': 'Pringles_FTT_Tubes', 'Score': 0})
        expected_util_result.append({'Group Name': 'Hula Hoops_LMP_Snacks', 'Score': 0})
        test_result_list = list()
        for exp_res in expected_util_result:
            test_result_list.append(
                len(block.util.block_results[(block.util.block_results['Group Name'] == exp_res['Group Name']) &
                                             (block.util.block_results['Score'] == exp_res[
                                                 'Score'])]) == 1)
        self.assertTrue(all(test_result_list))

    def create_scene_scif_matches_stitch_groups_data_mocks(self, test_case_file_path, scene_number):
        scif_test_case = pd.read_excel(test_case_file_path, sheet_name='scif')
        matches_test_case = pd.read_excel(test_case_file_path, sheet_name='matches')
        scif_scene = scif_test_case[scif_test_case['scene_fk'] == scene_number]
        matches_scene = matches_test_case[matches_test_case['scene_fk'] == scene_number]
        self.mock_scene_item_facts(scif_scene)
        self.mock_match_product_in_scene(matches_scene)
        # probe_group = self.mock_probe_group(pd.read_excel(test_case_file_path, sheetname='stitch_groups'))
        return matches_scene, scif_scene

    def test_number_of_shelves_kpi(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        num_of_shelves = NumberOfShelvesKpi(self.data_provider_mock, config_params={})
        num_of_shelves.calculate()
        kpi_result = pd.DataFrame(num_of_shelves.kpi_results)
        print kpi_result[['kpi_level_2_fk', 'numerator_id', 'result']]
        self.assertEquals(len(kpi_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 324, 'numerator_id': 2, 'result': 6})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_scene_kpis_are_not_calculated_if_location_secondary_shelf(self):
        matches, scene = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 3)

        num_of_shelves = NumberOfShelvesKpi(self.data_provider_mock, config_params={})
        num_of_shelves.calculate()
        kpi_results = pd.DataFrame(num_of_shelves.kpi_results)
        self.assertTrue(kpi_results.empty)

        num_of_bays = NumberOfBaysKpi(self.data_provider_mock, config_params={})
        num_of_bays.calculate()
        kpi_results = pd.DataFrame(num_of_bays.kpi_results)
        self.assertTrue(kpi_results.empty)

        num_of_facings = NumberOfFacingsKpi(self.data_provider_mock, config_params={})
        num_of_facings.calculate()
        kpi_result = pd.DataFrame(num_of_facings.kpi_results)
        self.assertTrue(kpi_result.empty)

        linear = LinearSpaceKpi(self.data_provider_mock, config_params={})
        linear.calculate()
        kpi_result = pd.DataFrame(linear.kpi_results)
        self.assertTrue(kpi_result.empty)

        placement_left = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                   config_params={"kpi_type": "Shelf Placement Vertical_Left"})
        placement_left.calculate()
        kpi_result = pd.DataFrame(placement_left.kpi_results)
        self.assertTrue(kpi_result.empty)

        placement_top = ShelfPlacementHorizontalKpi(self.data_provider_mock,
                                                    config_params={"kpi_type": "Placement by shelf numbers_Top"})
        placement_top.calculate()
        kpi_result = pd.DataFrame(placement_top.kpi_results)
        self.assertTrue(kpi_result.empty)

    def test_calculate_horizontal_placement_eye(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        eye = ShelfPlacementHorizontalKpi(self.data_provider_mock,
                                          config_params={"kpi_type": "Placement by shelf numbers_Eye"})
        eye.calculate()
        eye_result = pd.DataFrame(eye.kpi_results)
        eye_result['result'] = eye_result['result'].apply(lambda x: round(x, 5))
        eye_result['score'] = eye_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(eye_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 305, 'numerator_id': 2, 'denominator_id': 2, 'numerator_result': 2,
                              'denominator_result': 6, 'result': round(2.0/6*100, 5), 'score': round(2.0/6*100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(eye_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_horizontal_placement_mid(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        mid = ShelfPlacementHorizontalKpi(self.data_provider_mock,
                                          config_params={"kpi_type": "Placement by shelf numbers_Middle"})
        mid.calculate()
        mid_result = pd.DataFrame(mid.kpi_results)
        mid_result['result'] = mid_result['result'].apply(lambda x: round(x, 5))
        mid_result['score'] = mid_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(mid_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 306, 'numerator_id': 2, 'denominator_id': 2, 'numerator_result': 2,
                              'denominator_result': 6, 'result': round(2.0/6*100, 5), 'score': round(2.0/6*100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(mid_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_horizontal_placement_bottom(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        bottom = ShelfPlacementHorizontalKpi(self.data_provider_mock,
                                          config_params={"kpi_type": "Placement by shelf numbers_Bottom"})
        bottom.calculate()
        bottom_result = pd.DataFrame(bottom.kpi_results)
        bottom_result['result'] = bottom_result['result'].apply(lambda x: round(x, 5))
        bottom_result['score'] = bottom_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(bottom_result), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 307, 'numerator_id': 2, 'denominator_id': 2, 'numerator_result': 2,
                              'denominator_result': 6, 'result': round(2.0/6*100, 5), 'score': round(2.0/6*100, 5)})
        expected_list.append({'kpi_level_2_fk': 307, 'numerator_id': 3, 'denominator_id': 3, 'numerator_result': 1,
                              'denominator_result': 1, 'result': 1.0*100/1,
                              'score': 1.0*100/1})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(bottom_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_horizontal_placement_top(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
        top = ShelfPlacementHorizontalKpi(self.data_provider_mock,
                                          config_params={"kpi_type": "Placement by shelf numbers_Top"})
        top.calculate()
        top_result = pd.DataFrame(top.kpi_results)
        self.assertEquals(len(top_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 304, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 5,
                              'denominator_result': 5, 'result':  5.0/5*100, 'score':  5.0/5*100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(top_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_shelf_placement_vertical_mm_correcly_places_products_if_no_excluded_matches(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        # left
        vertical_left = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                  config_params={"kpi_type": "Shelf Placement Vertical_Left"})
        vertical_left.calculate()
        left_result = pd.DataFrame(vertical_left.kpi_results)
        self.assertEquals(len(left_result), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 325, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 2,
                              'denominator_result': 5, 'result': 2.0 / 5 * 100, 'score': 2.0 / 5 * 100})
        expected_list.append({'kpi_level_2_fk': 325, 'denominator_id': 2, 'numerator_result': 6,
                              'denominator_result': 6, 'numerator_id': 2, 'result': 6.0 / 6 * 100, 'score': 6.0 / 6 * 100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(left_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # center
        vertical_center = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                    config_params={"kpi_type": "Shelf Placement Vertical_Center"})
        vertical_center.calculate()
        center_result = pd.DataFrame(vertical_center.kpi_results)
        self.assertEquals(len(center_result), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 326, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 2,
                              'denominator_result': 5, 'result':  2.0 / 5 * 100, 'score':  2.0 / 5 * 100})
        expected_list.append({'kpi_level_2_fk': 326, 'denominator_id': 3, 'numerator_result': 1,
                              'denominator_result': 1, 'numerator_id': 3, 'result': 1.0 / 1 * 100, 'score': 1.0 / 1 * 100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(center_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # right
        vertical_right = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                   config_params={"kpi_type": "Shelf Placement Vertical_Right"})
        vertical_right.calculate()
        right_result = pd.DataFrame(vertical_right.kpi_results)
        self.assertEquals(len(right_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 327, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 1,
                              'denominator_result': 5, 'result': 1.0 / 5 * 100, 'score': 1.0 / 5 * 100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(right_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        all_results = left_result
        all_results = all_results.append(right_result)
        all_results = all_results.append(center_result)
        all_results = all_results.groupby(['numerator_id'], as_index=False).agg({'result': np.sum})
        self.assertTrue(all([round(res, 1) == 100] for res in all_results['result'].values.tolist()))

    def test_calculate_shelf_placement_vertical_mm_in_case_excluded_matches_exist_and_different_stitch_groups(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        # left
        vertical_left = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                  config_params={"kpi_type": "Shelf Placement Vertical_Left"})
        vertical_left.calculate()
        left_result = pd.DataFrame(vertical_left.kpi_results)
        left_result['result'] = left_result['result'].apply(lambda x: round(x, 5))
        left_result['score'] = left_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(left_result), 4)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 325, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 5,
                              'denominator_result': 7, 'result': round(5.0 / 7 * 100, 5), 'score': round(5.0 / 7 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 325, 'denominator_id': 2, 'numerator_result': 4,
                              'denominator_result': 6, 'numerator_id': 2, 'result': round(4.0 / 6 * 100, 5),
                              'score':round(4.0 / 6 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 325, 'numerator_id': 3, 'denominator_id': 3, 'numerator_result': 4,
                              'denominator_result': 8, 'result': round(4.0 / 8 * 100, 5), 'score': round(4.0 / 8 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 325, 'denominator_id': 4, 'numerator_result': 3,
                              'denominator_result': 6, 'numerator_id': 4, 'result': round(3.0 / 6 * 100, 5),
                              'score': round(3.0 / 6 * 100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(left_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # center
        vertical_center = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                    config_params={"kpi_type": "Shelf Placement Vertical_Center"})
        vertical_center.calculate()
        center_result = pd.DataFrame(vertical_center.kpi_results)
        center_result['result'] = center_result['result'].apply(lambda x: round(x, 5))
        center_result['score'] = center_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(center_result), 4)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 326, 'numerator_id': 1, 'denominator_id': 1, 'numerator_result': 2,
                              'denominator_result': 7, 'result': round(2.0 / 7 * 100, 5), 'score':round(2.0 / 7 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 326, 'denominator_id': 2, 'numerator_result': 2,
                              'denominator_result': 6, 'numerator_id': 2, 'result': round(2.0 / 6 * 100, 5),
                              'score': round(2.0 / 6 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 326, 'numerator_id': 3, 'denominator_id': 3, 'numerator_result': 3,
                              'denominator_result': 8, 'result': round(3.0 / 8 * 100, 5), 'score': round(3.0 / 8 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 326, 'denominator_id': 4, 'numerator_result': 2,
                              'denominator_result': 6, 'numerator_id': 4, 'result': round(2.0 / 6 * 100, 5),
                              'score': round(2.0 / 6 * 100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(center_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        # right
        vertical_right = ShelfPlacementVerticalKpi(self.data_provider_mock,
                                                   config_params={"kpi_type": "Shelf Placement Vertical_Right"})
        vertical_right.calculate()
        right_result = pd.DataFrame(vertical_right.kpi_results)
        right_result['result'] = right_result['result'].apply(lambda x: round(x, 5))
        right_result['score'] = right_result['score'].apply(lambda x: round(x, 5))
        self.assertEquals(len(right_result), 2)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 327, 'numerator_id': 4, 'denominator_id': 4, 'numerator_result': 1,
                              'denominator_result': 6, 'result': round(1.0 / 6 * 100, 5), 'score': round(1.0 / 6 * 100, 5)})
        expected_list.append({'kpi_level_2_fk': 327, 'numerator_id': 3, 'denominator_id': 3, 'numerator_result': 1,
                              'denominator_result': 8, 'result': round(1.0 / 8 * 100, 5), 'score': round(1.0 / 8 * 100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(right_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_number_of_bays_kpi(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        num_of_bays = NumberOfBaysKpi(self.data_provider_mock, config_params={})
        num_of_bays.calculate()
        kpi_result = pd.DataFrame(num_of_bays.kpi_results)
        print kpi_result[['kpi_level_2_fk', 'numerator_id', 'result']]
        self.assertEquals(len(kpi_result), 1)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 323, 'numerator_id': 2, 'result': 2})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_number_of_facings_kpi(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        num_of_facings = NumberOfFacingsKpi(self.data_provider_mock, config_params={})
        num_of_facings.calculate()
        kpi_result = pd.DataFrame(num_of_facings.kpi_results)
        print kpi_result[['kpi_level_2_fk', 'numerator_id', 'result']]
        self.assertEquals(len(kpi_result), 4)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 321, 'numerator_id': 1, 'result': 7})
        expected_list.append({'kpi_level_2_fk': 321, 'numerator_id': 2, 'result': 6})
        expected_list.append({'kpi_level_2_fk': 321, 'numerator_id': 3, 'result': 8})
        expected_list.append({'kpi_level_2_fk': 321, 'numerator_id': 4, 'result': 18})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_linear_kpi(self):
        matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
        self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)

        linear = LinearSpaceKpi(self.data_provider_mock, config_params={})
        linear.calculate()
        kpi_result = pd.DataFrame(linear.kpi_results)
        print kpi_result[['kpi_level_2_fk', 'numerator_id', 'result']]
        self.assertEquals(len(kpi_result), 4)
        expected_list = list()
        expected_list.append({'kpi_level_2_fk': 322, 'numerator_id': 1, 'result': 70})
        expected_list.append({'kpi_level_2_fk': 322, 'numerator_id': 2, 'result': 30})
        expected_list.append({'kpi_level_2_fk': 322, 'numerator_id': 3, 'result': 120})
        expected_list.append({'kpi_level_2_fk': 322, 'numerator_id': 4, 'result': 120})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(kpi_result, expected_result) == 1)
        self.assertTrue(all(test_result_list))

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

    # def test_get_available_hero_sku_list_retrieves_only_skus_in_store(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     util = PepsicoUtil(self.output, self.data_provider_mock)
    #     availability_sku = HeroAvailabilitySkuKpi(self.data_provider_mock, config_params={})
    #     availability_sku.calculate()
    #     hero_dependency_df = pd.DataFrame(availability_sku.kpi_results)
    #     hero_dependency_df['kpi_type'] = util.HERO_SKU_AVAILABILITY_SKU
    #     hero_list = util.get_available_hero_sku_list(hero_dependency_df)
    #     self.assertItemsEqual(hero_list, [1, 2])
    #
    # def test_sos_vs_target_targets_property(self):
    #     self.mock_scene_item_facts(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='scif'))
    #     self.mock_match_product_in_scene(pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='matches'))
    #     self.mock_scene_info(DataTestUnitPEPSICOUK.scene_info)
    #     self.mock_scene_kpi_results(DataTestUnitPEPSICOUK.scene_kpi_results_test_case_1)
    #     util = PepsicoUtil(self.output, self.data_provider_mock)
    #     self.assertItemsEqual(util.sos_vs_target_targets['pk'].values.tolist(), [21, 22, 23, 24, 25])
    #     expected_list = list()
    #     expected_list.append({'pk': 21, 'numerator_id': 2, 'denominator_id': 8})
    #     expected_list.append({'pk': 22, 'numerator_id': 2, 'denominator_id': 11})
    #     expected_list.append({'pk': 23, 'numerator_id': 155, 'denominator_id': 2})
    #     expected_list.append({'pk': 24, 'numerator_id': 10, 'denominator_id': 2})
    #     expected_list.append({'pk': 25, 'numerator_id': 1515, 'denominator_id': 2})
    #     test_result_list = []
    #     for expected_result in expected_list:
    #         test_result_list.append(self.check_df_values(util.sos_vs_target_targets, expected_result) == 1)
    #     self.assertTrue(all(test_result_list))

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
    #
    # def test_whatever_2(self):
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
    #     print tool_box.kpi_results_check
    #     #     print tool_box.scene_kpi_results
    #     #     print tool_box.scene_info