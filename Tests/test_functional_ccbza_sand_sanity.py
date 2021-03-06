#
# import os
#
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Testing.TestProjects import TestProjectsNames
# from Trax.Utils.Testing.Case import MockingTestCase
#
# from Tests.Data.TestData.test_data_ccbza_sand_sanity import ProjectsSanityData
# from Projects.CCBZA_SAND.Calculations import CCBZA_SANDCalculations
# from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
#
#
# __author__ = 'natalyak'
#
#
# class TestKEngineOutOfTheBox(TestFunctionalCase):
#
#     @property
#     def import_path(self):
#         return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
#
#     @property
#     def config_file_path(self):
#         return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
#
#     seeder = Seeder()
#
#     def _assert_kpi_results_filled(self):
#         connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
#         cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
#         cursor.execute('''
#         SELECT * FROM report.kpi_results
#         ''')
#         kpi_results = cursor.fetchall()
#         self.assertNotEquals(len(kpi_results), 0)
#         connector.disconnect_rds()
#
#     @seeder.seed(["ccbza_sand_seed"], ProjectsSanityData())
#     def test_ccbza_sand_sanity(self):
#         project_name = ProjectsSanityData.project_name
#         data_provider = KEngineDataProvider(project_name)
#         sessions = ['AD29338A-C2D9-4486-BD94-7B1E32224A11']
#         for session in sessions:
#             data_provider.load_session_data(session)
#             output = Output()
#             CCBZA_SANDCalculations(data_provider, output).run_project_calculations()
#             self._assert_kpi_results_filled()
