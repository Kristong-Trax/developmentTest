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
# from Tests.Data.TestData.test_data_bith_sanity import ProjectsSanityData
# from Projects.BITH.Calculations import BITHCalculations
# from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
#
#
# __author__ = 'jasmineg'
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
#         SELECT * FROM report.kpi_level_2_results
#         ''')
#         kpi_results = cursor.fetchall()
#         self.assertNotEquals(len(kpi_results), 0)
#         connector.disconnect_rds()
#
#     @seeder.seed(["bith_seed"], ProjectsSanityData())
#     def test_bith_sanity(self):
#         project_name = ProjectsSanityData.project_name
#         data_provider = KEngineDataProvider(project_name)
#         sessions = ['36704105-2E2A-498B-97DF-17FC97BCF835']
#         for session in sessions:
#             data_provider.load_session_data(session)
#             output = Output()
#             BITHCalculations(data_provider, output).run_project_calculations()
#             self._assert_kpi_results_filled()
