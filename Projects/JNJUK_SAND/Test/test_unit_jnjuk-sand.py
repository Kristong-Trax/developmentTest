#
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# import pandas as pd
# import Projects.RBUS_SAND.Tests.test_data as Data
# from Projects.RBUS_SAND.Utils.KPIToolBox import RBUSToolBox
#
# __author__ = 'idanr'
#
#
# class TestRbusSand(TestCase):
#
#     @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'rbus-sand'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         # self.test_objects = TestRbusSand()
#         # self.calc = RBUSCalculations(self.data_provider_mock, self.output)
#         # self.generator = RBUSGenerator(self.data_provider_mock, self.output)
#         # self.tool = RBUSToolBox(self.data_provider_mock, self.output)
#         self.data = Data
#         self.tool_box = RBUSToolBox(self.data_provider_mock, self.output)
#         # self.checks = Checks(self.data_provider_mock)
#
#
# class TestPlacementCount(TestRbusSand):
#
#     def test_df_head(self):
#         df = self.data.get_scene_data_head()
#         self.assertEquals(len(df.loc[df.template_name == 'ADDITIONAL AMBIENT PLACEMENT']),
#                          self.tool_box.get_scene_count(df, 'ADDITIONAL AMBIENT PLACEMENT'))
#
#     def test_num_of_scene_types_ambient(self):
#         df = self.data.get_scene_data_complete()
#         self.assertEquals(len(df.loc[df.template_name == 'ADDITIONAL AMBIENT PLACEMENT']),
#                           self.tool_box.get_scene_count(df, 'ADDITIONAL AMBIENT PLACEMENT'))
#
#     def test_num_of_scene_types_main_placement(self):
#         df = self.data.get_scene_data_complete()
#         self.assertEquals(len(df.loc[df.template_name == 'MAIN PLACEMENT']),
#                           self.tool_box.get_scene_count(df, 'MAIN PLACEMENT'))
#
#     def test_num_of_scene_types_non_exits(self):
#         df = self.data.get_scene_data_complete()
#         self.assertEquals(len(df.loc[df.template_name == 'ABC']),
#                           self.tool_box.get_scene_count(df, 'ABC'))
#
#
# class TestShelfOccupation(TestRbusSand):
#     def test_get_atomic_kpi(self):
#         kpi1 = 'K004'
#         kpi2 = 'K005'
#         df = self.data.get_atomic_fk()
#         self.assertLess(df[df['atomic_kpi_name'] == kpi1]['atomic_kpi_fk'].values[0],
#                         df[df['atomic_kpi_name'] == kpi2]['atomic_kpi_fk'].values[0])
#
#
# if __name__ == '__main__':
#     placement_count_tester = TestPlacementCount()
#     placement_count_tester.test_df_head()
#     placement_count_tester.test_num_of_scene_types_ambient()
#     placement_count_tester.test_num_of_scene_types_main_placement()
#     placement_count_tester.test_num_of_scene_types_non_exits()
#
#     shelf_occupation_tester = TestShelfOccupation()
#     shelf_occupation_tester.test_get_atomic_kpi()