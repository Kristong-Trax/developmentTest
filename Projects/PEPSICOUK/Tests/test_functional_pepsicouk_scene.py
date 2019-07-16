from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.PEPSICOUK.Tests.data_test_unit_pepsicouk import DataTestUnitPEPSICOUK
from Projects.PEPSICOUK.Utils.KPISceneToolBox import PEPSICOUKSceneToolBox
import pandas as pd
from pandas.util.testing import assert_frame_equal


__author__ = 'natalyak'


def get_exclusion_template_df_all_tests():
    template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
    return template_df


class Test_PEPSICOUKScene(TestFunctionalCase):
    seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.Utils.KPISceneToolBox'

    def set_up(self):
        super(Test_PEPSICOUKScene, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.db_users_mock = self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        # self.probe_group_mock = self.mock_probe_group()

        self.custom_entity_data_mock = self.mock_custom_entity_data()
        self.on_display_products_mock = self.mock_on_display_products()
        self.full_store_info = self.mock_store_data()

        self.exclusion_template_mock = self.mock_template_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        self.mock_all_products()
        self.mock_all_templates()
        self.mock_block()
        self.mock_adjacency()

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKCommonToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_block(self):
        self.mock_object('Block')

    def mock_adjacency(self):
        self.mock_object('Adjancency')

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
        static_kpi.return_value = DataTestUnitPEPSICOUK.kpi_static_data
        return static_kpi.return_value

    def mock_custom_entity_data(self):
        custom_entities = self.mock_object('PEPSICOUKCommonToolBox.get_custom_entity_data',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox')
        custom_entities.return_value = DataTestUnitPEPSICOUK.custom_entity
        return custom_entities.return_value

    def mock_on_display_products(self):
        on_display_products = self.mock_object('PEPSICOUKCommonToolBox.get_on_display_products',
                                               path='Projects.PEPSICOUK.Utils.CommonToolBox')
        on_display_products.return_value = DataTestUnitPEPSICOUK.on_display_products
        return on_display_products.return_value

    def mock_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox')
        template_data_mock.return_value = template_df
        return template_data_mock.return_value

    def mock_kpi_external_targets_data(self):
        # print DataTestUnitPEPSICOUK.external_targets
        external_targets_df = pd.read_excel(DataTestUnitPEPSICOUK.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

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

    def mock_all_products(self):
        self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
                                                                     sheetname='all_products')

    def mock_all_templates(self):
        self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def create_scene_scif_matches_stitch_groups_data_mocks(self, test_case_file_path, scene_number):
        scif_test_case = pd.read_excel(test_case_file_path, sheetname='scif')
        matches_test_case = pd.read_excel(test_case_file_path, sheetname='matches')
        scif_scene = scif_test_case[scif_test_case['scene_fk'] == scene_number]
        matches_scene = matches_test_case[matches_test_case['scene_fk'] == scene_number]
        self.mock_scene_item_facts(scif_scene)
        self.mock_match_product_in_scene(matches_scene)
        probe_group = self.mock_probe_group(pd.read_excel(test_case_file_path, sheetname='stitch_groups'))
        return probe_group, matches_scene, scif_scene

    def test_excluded_matches(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
                    DataTestUnitPEPSICOUK.test_case_1, 1)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        expected_result = set(matches[~(matches['Out'].isnull())]['probe_match_fk'].values.tolist())
        self.assertItemsEqual(expected_result, scene_tb.excluded_matches)

    def test_main_calculation_does_not_happen_if_location_secondary_shelf(self):
        probe_group, matches, scene = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 3)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.main_function()
        self.assertTrue(scene_tb.kpi_results.empty)

    def test_calculate_number_of_facings_and_linear_space(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_number_of_facings_and_linear_space()
        expected_list = []
        expected_list.append({'kpi_fk': 321, 'numerator': 1, 'result': 7})
        expected_list.append({'kpi_fk': 322, 'numerator': 1, 'result': 70})
        expected_list.append({'kpi_fk': 321, 'numerator': 2, 'result': 6})
        expected_list.append({'kpi_fk': 322, 'numerator': 2, 'result': 30})
        expected_list.append({'kpi_fk': 321, 'numerator': 3, 'result': 8})
        expected_list.append({'kpi_fk': 322, 'numerator': 3, 'result': 120})
        expected_list.append({'kpi_fk': 321, 'numerator': 4, 'result': 18})
        expected_list.append({'kpi_fk': 322, 'numerator': 4, 'result': 120})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_number_of_bays_and_shelves(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_number_of_bays_and_shelves()
        expected_list = []
        expected_list.append({'kpi_fk': 323, 'numerator': 2, 'result': 2})
        expected_list.append({'kpi_fk': 324, 'numerator': 2, 'result': 6})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_get_scene_bay_max_shelves_retrieves_expected_df_in_case_of_8_shelves(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        expected_result = pd.DataFrame(
            [{u'kpi_level_2_fk': 304, u'type': PEPSICOUKSceneToolBox.PLACEMENT_BY_SHELF_NUMBERS_TOP,
              u'No of Shelves in Fixture (per bay) (key)': 8, u'Shelves From Bottom To Include (data)': '7,8',
             u'shelves_all_placements': '7,8,5,6,3,4,1,2'},
             {u'kpi_level_2_fk': 305, u'type': 'Placement by shelf numbers_Eye',
              u'No of Shelves in Fixture (per bay) (key)': 8, u'Shelves From Bottom To Include (data)': '5,6',
              u'shelves_all_placements': '7,8,5,6,3,4,1,2'},
             {u'kpi_level_2_fk': 306, u'type': 'Placement by shelf numbers_Middle',
              u'No of Shelves in Fixture (per bay) (key)': 8, u'Shelves From Bottom To Include (data)': '3,4',
              u'shelves_all_placements': '7,8,5,6,3,4,1,2'},
             {u'kpi_level_2_fk': 307, u'type': 'Placement by shelf numbers_Bottom',
              u'No of Shelves in Fixture (per bay) (key)': 8, u'Shelves From Bottom To Include (data)': '1,2',
              u'shelves_all_placements': '7,8,5,6,3,4,1,2'}])
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        external_targets = scene_tb.commontools.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == scene_tb.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = scene_tb.get_scene_bay_max_shelves(shelf_placmnt_targets)
            bay_max_shelves = bay_max_shelves[['kpi_level_2_fk', 'type', 'No of Shelves in Fixture (per bay) (key)',
                                               'Shelves From Bottom To Include (data)', 'shelves_all_placements']]
            assert_frame_equal(expected_result.sort_index(axis=1), bay_max_shelves.sort_index(axis=1),
                               check_dtype=False, check_column_type=False, check_names=True)

    def test_get_scene_bay_max_shelves_retrieves_expected_df_in_case_of_several_bays(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        expected_result = pd.DataFrame([
             {u'kpi_level_2_fk': 304, u'type': PEPSICOUKSceneToolBox.PLACEMENT_BY_SHELF_NUMBERS_TOP,
              u'No of Shelves in Fixture (per bay) (key)': 6, u'Shelves From Bottom To Include (data)': '6',
             u'shelves_all_placements': '6,4,5,2,3,1'},
             {u'kpi_level_2_fk': 305, u'type': 'Placement by shelf numbers_Eye',
              u'No of Shelves in Fixture (per bay) (key)': 6, u'Shelves From Bottom To Include (data)': '4,5',
              u'shelves_all_placements': '6,4,5,2,3,1'},
             {u'kpi_level_2_fk': 306, u'type': 'Placement by shelf numbers_Middle',
              u'No of Shelves in Fixture (per bay) (key)': 6, u'Shelves From Bottom To Include (data)': '2,3',
              u'shelves_all_placements': '6,4,5,2,3,1'},
             {u'kpi_level_2_fk': 307, u'type': 'Placement by shelf numbers_Bottom',
              u'No of Shelves in Fixture (per bay) (key)': 6, u'Shelves From Bottom To Include (data)': '1',
              u'shelves_all_placements': '6,4,5,2,3,1'},
            {u'kpi_level_2_fk': 304, u'type': PEPSICOUKSceneToolBox.PLACEMENT_BY_SHELF_NUMBERS_TOP,
             u'No of Shelves in Fixture (per bay) (key)': 3, u'Shelves From Bottom To Include (data)': '3',
             u'shelves_all_placements': '3,2,1'},
            {u'kpi_level_2_fk': 305, u'type': 'Placement by shelf numbers_Eye',
             u'No of Shelves in Fixture (per bay) (key)': 3, u'Shelves From Bottom To Include (data)': '2',
             u'shelves_all_placements': '3,2,1'},
            {u'kpi_level_2_fk': 307, u'type': 'Placement by shelf numbers_Bottom',
             u'No of Shelves in Fixture (per bay) (key)': 3, u'Shelves From Bottom To Include (data)': '1',
             u'shelves_all_placements': '3,2,1'}
        ])
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        external_targets = scene_tb.commontools.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == scene_tb.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = scene_tb.get_scene_bay_max_shelves(shelf_placmnt_targets)
            bay_max_shelves = bay_max_shelves[['kpi_level_2_fk', 'type', 'No of Shelves in Fixture (per bay) (key)',
                                               'Shelves From Bottom To Include (data)', 'shelves_all_placements']]
            assert_frame_equal(expected_result.sort_index(axis=1), bay_max_shelves.sort_index(axis=1),
                               check_dtype=False, check_column_type=False, check_names=True)

    def test_get_scene_bay_max_shelves_retrieves_empty_df_in_case_of_excluded_shelves(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 3)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        external_targets = scene_tb.commontools.all_targets_unpacked
        shelf_placmnt_targets = external_targets[external_targets['operation_type'] == scene_tb.SHELF_PLACEMENT]
        if not shelf_placmnt_targets.empty:
            bay_max_shelves = scene_tb.get_scene_bay_max_shelves(shelf_placmnt_targets)
            self.assertTrue(bay_max_shelves.empty)

    def test_calculate_horizontal_placement(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        expected_list = []
        expected_list.append({'kpi_fk': 304, 'numerator': 1, 'result': 5.0/5*100})
        expected_list.append({'kpi_fk': 307, 'numerator': 2, 'result': round(2.0/6*100, 5)})
        expected_list.append({'kpi_fk': 306, 'numerator': 2, 'result': round(2.0/6*100, 5)})
        expected_list.append({'kpi_fk': 305, 'numerator': 2, 'result': round(2.0/6*100, 5)})
        expected_list.append({'kpi_fk': 307, 'numerator': 3, 'result': 1.0*100/1})
        scene_tb.calculate_shelf_placement_horizontal()
        kpi_results = scene_tb.kpi_results
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_horizontal_placement_does_not_calculate_kpis_if_irrelevant_scene(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 3)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_shelf_placement_horizontal()
        self.assertTrue(scene_tb.kpi_results.empty)

    def test_calculate_shelf_placement_vertical_mm_correcly_places_products_if_no_excluded_matches(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 2)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_shelf_placement_vertical_mm()
        expected_list = []
        expected_list.append({'kpi_fk': 327, 'numerator': 1, 'result': 1.0 / 5 * 100})
        expected_list.append({'kpi_fk': 326, 'numerator': 1, 'result': 2.0 / 5 * 100})
        expected_list.append({'kpi_fk': 325, 'numerator': 1, 'result': 2.0 / 5 * 100})
        expected_list.append({'kpi_fk': 325, 'numerator': 2, 'result': 6.0 / 6 * 100})
        expected_list.append({'kpi_fk': 326, 'numerator': 3, 'result': 1.0 / 1 * 100})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_shelf_placement_vertical_mm_does_not_calculate_kpis_if_all_matches_excluded(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 3)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_shelf_placement_vertical_mm()
        self.assertTrue(scene_tb.kpi_results.empty)

    def test_calculate_shelf_placement_vertical_mm_in_case_excluded_matches_exist_and_different_stitch_groups(self):
        probe_group, matches, scif = self.create_scene_scif_matches_stitch_groups_data_mocks(
            DataTestUnitPEPSICOUK.test_case_1, 1)
        scene_tb = PEPSICOUKSceneToolBox(self.data_provider_mock, self.output)
        scene_tb.calculate_shelf_placement_vertical_mm()
        kpi_results = scene_tb.kpi_results
        kpi_results['result'] = kpi_results['result'].apply(lambda x: round(x, 5))
        expected_list = []
        expected_list.append({'kpi_fk': 327, 'numerator': 4, 'result': round(1.0 / 6 * 100, 5)})
        expected_list.append({'kpi_fk': 326, 'numerator': 4, 'result': round(2.0 / 6 * 100, 5)})
        expected_list.append({'kpi_fk': 325, 'numerator': 4, 'result': round(3.0 / 6 * 100, 5)})
        expected_list.append({'kpi_fk': 325, 'numerator': 1, 'result': round(5.0 / 7 * 100, 5)})
        expected_list.append({'kpi_fk': 326, 'numerator': 1, 'result': round(2.0 / 7 * 100, 5)})
        expected_list.append({'kpi_fk': 325, 'numerator': 2, 'result': round(4.0 / 6 * 100, 5)})
        expected_list.append({'kpi_fk': 326, 'numerator': 2, 'result': round(2.0 / 6 * 100, 5)})
        expected_list.append({'kpi_fk': 325, 'numerator': 3, 'result': round(4.0 / 8 * 100, 5)})
        expected_list.append({'kpi_fk': 326, 'numerator': 3, 'result': round(3.0 / 8 * 100, 5)})
        expected_list.append({'kpi_fk': 327, 'numerator': 3, 'result': round(1.0 / 8 * 100, 5)})
        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        non_expected_results = []
        non_expected_results.append({'kpi_fk': 325, 'numerator': 0})
        non_expected_results.append({'kpi_fk': 326, 'numerator': 0})
        non_expected_results.append({'kpi_fk': 327, 'numerator': 0})
        test_result_list = []
        for result in non_expected_results:
            test_result_list.append(self.check_kpi_results(scene_tb.kpi_results, result) == 0)
        self.assertTrue(all(test_result_list))

    def check_kpi_results(self, kpi_results_df, expected_results_dict):
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