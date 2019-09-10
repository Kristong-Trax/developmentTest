import os
import pickle

from mock import patch
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Projects.PNGCNTEST.ShareOfDisplay.Calculation import calculate_share_of_display
from Projects.PNGCNTEST.ShareOfDisplay.Tests.data_functional_test_share_of_display import \
    InsertDataIntoMySqlProjectSOD, SodTestSeedData
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.PNGCNTEST.ShareOfDisplay.Tests.ShareOfDisaplyTestsQueries import \
    get_display_surface_by_scene_query, get_display_item_facts_by_scene_query
import pandas as pd
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import TestCase
from Trax.Utils.Testing.ConfigTest import TempTestConfig

__author__ = 'Dudi S'

MOCK_PATH = 'Trax.Analytics.Calculation.ShareOfDisplay.Calculation'


class TestShareOfDisplay(TestFunctionalCase):
    _project1 = TestProjectsNames().TEST_PROJECT_1
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'k-engine-test.config')
    seeder = Seeder()
    _data_input_project_1 = None
    patcher = None

    @classmethod
    def mock_empty_spaces(cls):
        cls.patcher = patch('{}.EmptySpaceKpiGenerator'.format(MOCK_PATH))
        cls.patcher.start()


    @classmethod
    @seeder.seed(['sql_sod'], SodTestSeedData())
    def setUpClass(cls):
        super(TestShareOfDisplay, cls).setUpClass()
        cls.mock_empty_spaces()
        cls._data_input_project_1 = InsertDataIntoMySqlProjectSOD(cls._project1)
        cls._data_input_project_1.update_all_scenes_to_same_session()
        with TempTestConfig(conf_file=cls.config_file):
            data_provider = KEngineDataProvider(cls._project1)
            data_provider.load_session_data('2b14c6c6-1458-4c3c-96a2-1ae20824f054')
            calculate_share_of_display(None, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', data_provider=data_provider)
        data_provider.project_connector.db.commit()

    @classmethod
    def tearDownClass(cls):
        cls.seeder.stop()
        cls.patcher.stop()
    #
    # def assert_actual_to_expected_from_file(self, actual, test):
    #     with open(os.path.dirname(__file__) + '/expected.pickle', 'rb') as handle:
    #         b = pickle.load(handle)
    #     expected = b[test]
    #     result = actual.equals(expected)
    #     self.assertEqual(result, True)
    #
    # def test_display_size_and_kind_for_non_branded_cube_with_total(self):
    #     query = get_display_surface_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_non_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_branded_cube_with_total(self):
    #     query = get_display_surface_by_scene_query(431939)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_branded_and_non_branded_cube_with_total(self):
    #     query = get_display_surface_by_scene_query(438154)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_'
    #                                                      'branded_and_non_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_promotion_wall(self):
    #     query = get_display_surface_by_scene_query(436955)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_promotion_wall')
    #
    # def test_display_size_and_kind_for_multiple_regular_displays(self):
    #     query = get_display_surface_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_multiple_regular_displays')
    #
    # def test_display_size_and_kind_for_one_regular_and_cube(self):
    #     query = get_display_surface_by_scene_query(432600)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_one_regular_and_cube')
    #
    # def test_display_item_facts_for_non_branded_cube_with_total(self):
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_for_non_branded_cube')
    #
    # def test_display_item_facts_non_cube_non_promotion_wall_display(self):
    #     query = get_display_item_facts_by_scene_query(433255)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_non_cube_non_promotion_wall_display')
    #
    # def test_display_item_facts_branded_cube_with_total(self):
    #     query = get_display_item_facts_by_scene_query(431939)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_branded_cube_with_total')
    #
    # def test_display_item_facts_branded_and_not_cube_with_total(self):
    #     query = get_display_item_facts_by_scene_query(438154)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_branded_and_not_cube_with_total')
    #
    # def test_display_item_facts_promotion_wall_with_multiple_bays(self):
    #     query = get_display_item_facts_by_scene_query(436955)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_promotion_wall_with_multiple_bays')
    #
    # def test_display_item_facts_multiple_non_cube_displays(self):
    #     query = get_display_item_facts_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     display_surface_fks = list(set(actual['display_surface_fk'].tolist()))
    #     display_surface_fks.sort()
    #     i = 200
    #     for display_surface in display_surface_fks:
    #         actual.loc[actual['display_surface_fk'] == display_surface, 'display_surface_fk'] = i
    #         i += 1
    #     actual.drop('pk', axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_multiple_non_cube_displays_display_1')
    #
    # def test_display_item_facts_one_regular_and_cube(self):
    #     query = get_display_item_facts_by_scene_query(432600)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     display_surface_fks = list(set(actual['display_surface_fk'].tolist()))
    #     display_surface_fks.sort()
    #     i = 200
    #     for display_surface in display_surface_fks:
    #         actual.loc[actual['display_surface_fk'] == display_surface, 'display_surface_fk'] = i
    #         i += 1
    #     actual.drop('pk', axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_one_regular_and_cube')
    #
    # def test_display_item_facts_sos_type_linear(self):
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'product_size'].values[0], 0.1)
    #
    # def test_display_item_facts_sos_type_facings(self):
    #     query = get_display_item_facts_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1385, 'product_size'].values[0], 0.38)
    #
    # def test_display_item_cube_display_calculate_product_size_only_for_bottom_shelf_products(self):
    #     query = get_display_item_facts_by_scene_query(437833)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1460, 'product_size'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1214, 'product_size'].values[0], 0)
    #
    # def test_display_item_cube_product_size_ignore_stacking(self):
    #     query = get_display_item_facts_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 724, 'product_size'].values[0], 0.06)
    #
    # def test_display_item_facts_calculate_in_sos_correctly(self):
    #     """
    #     Test one item which is should be included and one that should be excluded.
    #     """
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'in_sos'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1212, 'in_sos'].values[0], 0)
    #
    # def test_display_item_facts_calculate_own_manufacturer_correctly(self):
    #     """
    #     Test own manufacturer calculated correctly
    #     """
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 722, 'own'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 728, 'own'].values[0], 0)
    #
    # def test_display_item_facts_cube_with_display_tag_in_the_same_bay_are_counted_once(self):
    #     """
    #     Test facings field for cube is correct for when there few cube tags share the same bay
    #     """
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'facings'].values[0], 7)

        # def test_display_without_valid_tag_has_no_item_facts(self):
        #     """
        #     Test a display with a tag on bay -1 has no item facts
        #     """
        #     calculate_share_of_display(self._data_input_project_1._conn, 'fec30d8c-f0c7-48ec-b137-e2f4dc02f8f1')
        #     query = get_display_item_facts_by_scene_query(433027)
        #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
        #     display_surface_pk = actual['display_surface_pk'].drop_duplicates('display_surface_pk', inplace=1)
        #     self.assertEqual(len(display_surface_pk.index), 4)
