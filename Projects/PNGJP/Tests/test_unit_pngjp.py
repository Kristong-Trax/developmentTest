# from mock import MagicMock, mock
from KPIUtils_v2.Utils.Consts.OldDB import KpiResults
# from Projects.PNGJP.Utils.KpiQualitative import PNGJPKpiQualitative_ToolBox
# from Trax.Utils.Testing.Case import TestCase
#
# import pandas as pd
# import Projects.PNGJP.Tests.test_data as Data
#
# __author__ = 'israels'
#
#
# class PNGJP_SAND2TestRbusSand(TestCase):
#
#     @mock.patch('Projects.PNGJP.Utils.KpiQualitative.ProjectConnector')
#     def setUp(self, x):
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'pngjp'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         # self.test_objects = PNGJP_SAND2TestRbusSand()
#         # self.calc = RBUSCalculations(self.data_provider_mock, self.output)
#         # self.generator = RBUSGenerator(self.data_provider_mock, self.output)
#         # self.tool = RBUSToolBox(self.data_provider_mock, self.output)
#         self.data = Data
#         self.tool_box = PNGJPKpiQualitative_ToolBox(self.data_provider_mock, self.output)
#         # self.checks = Checks(self.data_provider_mock)
#
#
# class PNGJP_SAND2TestPlacementCount(PNGJP_SAND2TestRbusSand):
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
# class PNGJP_SAND2TestShelfOccupation(PNGJP_SAND2TestRbusSand):
#     def test_get_atomic_kpi(self):
#         kpi1 = 'K004'
#         kpi2 = 'K005'
#         df = self.data.get_atomic_fk()
#         self.assertLess(df[df['atomic_kpi_name'] == kpi1][KpiResults.ATOMIC_KPI_FK].values[0],
#                         df[df['atomic_kpi_name'] == kpi2][KpiResults.ATOMIC_KPI_FK].values[0])
#
#
# if __name__ == '__main__':
#     placement_count_tester = PNGJP_SAND2TestPlacementCount()
#     placement_count_tester.test_df_head()
#     placement_count_tester.test_num_of_scene_types_ambient()
#     placement_count_tester.test_num_of_scene_types_main_placement()
#     placement_count_tester.test_num_of_scene_types_non_exits()
#
#     shelf_occupation_tester = PNGJP_SAND2TestShelfOccupation()
#     shelf_occupation_tester.test_get_atomic_kpi()
