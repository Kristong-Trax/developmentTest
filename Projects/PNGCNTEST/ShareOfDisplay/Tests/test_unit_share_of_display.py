import os
import pickle

from mock import patch

from Projects.PNGCNTEST.ShareOfDisplay.Calculation import calculate_share_of_display, PNGShareOfDisplay
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import TestCase
from Projects.PNGCNTEST.ShareOfDisplay.Tests.data_functional_test_share_of_display import \
    InsertDataIntoMySqlProjectSOD, SodTestSeedData
from Trax.Data.Testing.SeedNew import Seeder
from Projects.PNGCNTEST.ShareOfDisplay.Tests.ShareOfDisaplyTestsQueries import get_display_surface_by_scene_query, \
    get_display_surface_by_session_query, \
    get_display_item_facts_by_scene_query
import pandas as pd

__author__ = 'Dudi S'

MOCK_PATH = 'Projects.PNGCNTEST.ShareOfDisplay.Calculation'


class TestShareOfDisplay(TestFunctionalCase):
    _project1 = TestProjectsNames().TEST_PROJECT_1
    data_provider = None
    seeder = Seeder()
    patcher = None

    @classmethod
    def mock_empty_spaces(cls):
        cls.patcher = patch('{}.EmptySpaceKpiGenerator'.format(MOCK_PATH))
        cls.patcher.start()

    @classmethod
    @seeder.seed(['sql_sod'], SodTestSeedData())
    def setUpClass(cls):
        super(TestShareOfDisplay, cls).setUpClass()
        cls._data_input_project_1 = InsertDataIntoMySqlProjectSOD(cls._project1)
        cls.mock_empty_spaces()

    @classmethod
    def tearDownClass(cls):
        cls.seeder.stop()
        cls.patcher.stop()

    def assert_actual_to_expected_from_file(self, actual, test):
        with open(os.path.dirname(__file__) + '/expected.pickle', 'rb') as handle:
            b = pickle.load(handle)
        expected = b[test]
        result = actual.equals(expected)
        self.assertEqual(result, True)

    # def test_display_size_and_kind_for_non_branded_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_surface_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_non_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_branded_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, 'ff555f6c-4ed0-4c33-a24a-d0825e18b817', None)
    #     query = get_display_surface_by_scene_query(431939)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_branded_and_non_branded_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '05ca064a-5a5e-4e65-858a-ac7f2c09d059', None)
    #     query = get_display_surface_by_scene_query(438154)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_branded'
    #                                                      '_and_non_branded_cube_with_total')
    #
    # def test_display_size_and_kind_for_promotion_wall(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4f2a8418-6e20-427a-a1f7-6046276b73e7', None)
    #     query = get_display_surface_by_scene_query(436955)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_promotion_wall')
    #
    # def test_display_size_and_kind_for_multiple_regular_displays(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4c7833f5-3bc7-41d4-997f-b381c4b41a41', None)
    #     query = get_display_surface_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_multiple_regular_displays')
    #
    # def test_display_size_and_kind_for_one_regular_and_cube(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, 'c61a8690-4761-4389-9f74-c1922115b23e', None)
    #     query = get_display_surface_by_scene_query(432600)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_size_and_kind_for_one_regular_and_cube')
    #
    # def test_previous_data_is_deleted_correctly(self):
    #     session = '2b14c6c6-1458-4c3c-96a2-1ae20824f054'
    #     calculate_share_of_display(self._data_input_project_1._conn, session, None)
    #     query = get_display_surface_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(len(actual.index), 1)
    #     sod = PNGShareOfDisplay(self._data_input_project_1._conn, session, data_provider=None)
    #     sod._delete_previous_data()
    #     query = get_display_surface_by_session_query(session)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(len(actual.index), 0)
    #
    # def test_display_item_facts_for_non_branded_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_for_non_branded_cube')
    #
    # def test_display_item_facts_non_cube_non_promotion_wall_display(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4a6921ff-02f5-46f6-b8ed-ca38f2f8fc36', None)
    #     query = get_display_item_facts_by_scene_query(433255)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_non_cube_non_promotion_wall_display')
    #
    # def test_display_item_facts_branded_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, 'ff555f6c-4ed0-4c33-a24a-d0825e18b817', None)
    #     query = get_display_item_facts_by_scene_query(431939)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_branded_cube_with_total')
    #
    # def test_display_item_facts_branded_and_not_cube_with_total(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '05ca064a-5a5e-4e65-858a-ac7f2c09d059', None)
    #     query = get_display_item_facts_by_scene_query(438154)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_branded_and_not_cube_with_total')
    #
    # def test_display_item_facts_promotion_wall_with_multiple_bays(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4f2a8418-6e20-427a-a1f7-6046276b73e7', None)
    #     query = get_display_item_facts_by_scene_query(436955)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assert_actual_to_expected_from_file(actual, 'test_display_item_facts_promotion_wall_with_multiple_bays')
    #
    # def test_display_item_facts_multiple_non_cube_displays(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4c7833f5-3bc7-41d4-997f-b381c4b41a41', None)
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
    #     calculate_share_of_display(self._data_input_project_1._conn, 'c61a8690-4761-4389-9f74-c1922115b23e', None)
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
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     actual.drop(['pk', 'display_surface_fk'], axis=1, inplace=1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'product_size'].values[0], 0.1)
    #
    # def test_display_item_facts_sos_type_facings(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4c7833f5-3bc7-41d4-997f-b381c4b41a41', None)
    #     query = get_display_item_facts_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1385, 'product_size'].values[0], 0.38)
    #
    # def test_display_item_cube_display_calculate_product_size_only_for_bottom_shelf_products(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '545b8dae-1b76-472f-b0fa-f6a101c4f4ad', None)
    #     query = get_display_item_facts_by_scene_query(437833)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1460, 'product_size'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1214, 'product_size'].values[0], 0)
    #
    # def test_display_item_cube_product_size_ignore_stacking(self):
    #     calculate_share_of_display(self._data_input_project_1._conn, '4c7833f5-3bc7-41d4-997f-b381c4b41a41', None)
    #     query = get_display_item_facts_by_scene_query(435070)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 724, 'product_size'].values[0], 0.06)
    #
    # def test_display_item_facts_calculate_in_sos_correctly(self):
    #     """
    #     Test one item which is should be included and one that should be excluded.
    #     """
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'in_sos'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1212, 'in_sos'].values[0], 0)
    #
    # def test_display_item_facts_calculate_own_manufacturer_correctly(self):
    #     """
    #     Test own manufacturer calculated correctly
    #     """
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 722, 'own'].values[0], 1)
    #     self.assertEqual(actual.loc[actual['item_id'] == 728, 'own'].values[0], 0)
    #
    # def test_display_item_facts_cube_with_display_tag_in_the_same_bay_are_counted_once(self):
    #     """
    #     Test facings field for cube is correct for when there few cube tags share the same bay
    #     """
    #     calculate_share_of_display(self._data_input_project_1._conn, '2b14c6c6-1458-4c3c-96a2-1ae20824f054', None)
    #     query = get_display_item_facts_by_scene_query(431539)
    #     actual = pd.read_sql_query(query, self._data_input_project_1._conn.db)
    #     self.assertEqual(actual.loc[actual['item_id'] == 1156, 'facings'].values[0], 7)
