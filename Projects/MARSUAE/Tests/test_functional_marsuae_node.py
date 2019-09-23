from Projects.MARSUAE.Utils.Nodes import Node
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
# from Tests.TestUtils import remove_cache_and_storage
from Projects.MARSUAE.Tests.data_test_unit_marsuae_sand import DataTestNode


class TestNode(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Projects.MARSUAE_SAND.Utils.Nodes'

    def set_up(self):
        super(TestNode, self).set_up()

    def test_get_kpi_execute_list_returns_correct_order_kpi_list_if_all_kpis_are_connected(self):
        expected_result = ['three', 'five', 'six', 'two', 'four', 'one']
        result = Node.get_kpi_execute_list(DataTestNode.atomics_df_1)
        self.assertEqual(expected_result, result)

    def test_get_kpi_execute_list_returns_correct_order_kpi_list_if_non_hierarchy_items(self):
        expected_result = ['seven', 'five', 'three', 'six', 'two', 'four', 'one']
        result = Node.get_kpi_execute_list(DataTestNode.atomics_df_2)
        self.assertEqual(expected_result, result)

    def test_get_kpi_execute_list_runs_without_errors_if_df_has_no_items_to_order(self):
        expected_result = ['three', 'two', 'four', 'one']
        result = Node.get_kpi_execute_list(DataTestNode.atomics_df_3)
        self.assertItemsEqual(expected_result, result)