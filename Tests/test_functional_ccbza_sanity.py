#
# import os
# import MySQLdb
#
# from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Testing.TestProjects import TestProjectsNames
# from Trax.Utils.Testing.Case import MockingTestCase
#
# from Tests.Data.TestData.test_data_ccbza_sanity import ProjectsSanityData
# from Projects.CCBZA.Calculations import CCBZA_Calculations
# from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase
#
#
# __author__ = 'natalyak'
#
#
# class TestKEngineOutOfTheBox(TestMockingFunctionalCase):
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
#         connector = ProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
#         cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
#         cursor.execute('''
#         SELECT * FROM report.kpi_results
#         ''')
#         kpi_results = cursor.fetchall()
#         self.assertNotEquals(len(kpi_results), 0)
#         connector.disconnect_rds()
#
#     @seeder.seed(["ccbza_seed"], ProjectsSanityData())
#     def test_ccbza_sanity(self):
#         project_name = ProjectsSanityData.project_name
#         data_provider = KEngineDataProvider(project_name)
#         sessions = ['02BCE755-24B6-4021-A4C0-2F38D86D8638']
#         for session in sessions:
#             data_provider.load_session_data(session)
#             output = Output()
#             CCBZA_Calculations(data_provider, output).run_project_calculations()
#             self._assert_kpi_results_filled()
