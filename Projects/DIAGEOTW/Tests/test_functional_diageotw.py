import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.DIAGEOTW.Calculations import DIAGEOTWCalculations
from Projects.DIAGEOTW.Tests.Data.test_data_diageotw import ProjectsSanityData
from Projects.DIAGEOTW.Utils.KPIToolBox import DIAGEOTWToolBox
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'


class TestDiageotw(TestFunctionalCase):
    seeder = Seeder()

    def set_up(self):
        super(TestDiageotw, self).set_up()
        self.project_name = ProjectsSanityData.project_name
        self.output = Output()
        self.mock_object('save_latest_templates', path='KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox')
        self.session_uid = 'B1F11D51-DC00-4FDB-86E4-B389530C66DB'
        remove_cache_and_storage()

#     @property
#     def import_path(self):
#         return 'Projects.DIAGEOTW.Utils.KPIToolBox'
#
#     @property
#     def import_path(self):
#         return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
#
#     # @seeder.seed(["mongodb_products_and_brands_seed", "diageotw_seed"], ProjectsSanityData())
#     # def test_diageotw_sanity(self):
#     #     data_provider = KEngineDataProvider(self.project_name)
#     #     data_provider.load_session_data(self.session_uid)
#     #     DIAGEOTWCalculations(data_provider, self.output).run_project_calculations()
#
#     @seeder.seed(["mongodb_products_and_brands_seed", "diageotw_seed"], ProjectsSanityData())
#     def test_get_kpi_static_data_return_type(self):
#         """
#         test the return value of "get_kpi_static_data"
#         return error massage if its not 'pd.DataFrame' type
#         """
#         data_provider = KEngineDataProvider(self.project_name)
#         data_provider.load_session_data(self.session_uid)
#         tool_box = DIAGEOTWToolBox(data_provider, self.output)
#         result = tool_box.get_kpi_static_data()
#         expected_result = pd.DataFrame
#         self.assertIsInstance(result, expected_result)
#
#     @seeder.seed(["mongodb_products_and_brands_seed", "diageotw_seed"], ProjectsSanityData())
#     def test_get_match_display_return_type(self):
#         """
#         test the return value of "get_kpi_static_data"
#         return error massage if its not 'pd.DataFrame' type
#         """
#         data_provider = KEngineDataProvider(self.project_name)
#         data_provider.load_session_data(self.session_uid)
#         tool_box = DIAGEOTWToolBox(data_provider, self.output)
#         result = tool_box.get_kpi_static_data()
#         expected_result = pd.DataFrame
#         self.assertIsInstance(result, expected_result)
#
# # _get_direction_for_relative_position(self, value)
#     @seeder.seed(["mongodb_products_and_brands_seed", "diageotw_seed"], ProjectsSanityData())
#     def test_get_direction_for_relative_position_input(self):
#         """
#         compere different types of inputs with expected result
#         """
#         data_provider = KEngineDataProvider(self.project_name)
#         data_provider.load_session_data(self.session_uid)
#         tool_box = DIAGEOTWToolBox(data_provider, self.output)
#
#         value = -8 # negative number
#         expected_result = 0
#         result = tool_box._get_direction_for_relative_position(value)
#         self.assertEqual(result, expected_result)
#
#         value = 123  # valid number
#         expected_result = 123
#         result = tool_box._get_direction_for_relative_position(value)
#         self.assertEqual(result, expected_result)
#
#         value = 'General'  # valide string
#         expected_result = 1000
#         result = tool_box._get_direction_for_relative_position(value)
#         self.assertEqual(result, expected_result)
#
#         value = 'str'  # invalide string
#         expected_result = 0
#         result = tool_box._get_direction_for_relative_position(value)
#         self.assertEqual(result, expected_result)
#
#         value = '123'  # valid string representing a number
#         expected_result = 123
#         result = tool_box._get_direction_for_relative_position(value)
#         self.assertEqual(result, expected_result)
