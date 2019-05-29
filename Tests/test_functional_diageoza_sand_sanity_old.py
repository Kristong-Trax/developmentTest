#
# import os
# import MySQLdb
#
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Testing.TestProjects import TestProjectsNames
# from Trax.Utils.Testing.Case import MockingTestCase
# from mock import patch
#
# from Tests.Data.Templates.diageoza.MPA import mpa
# from Tests.Data.Templates.diageoza.NewProducts import products
# from Tests.Data.Templates.diageoza.POSM import posm
# from Tests.Data.TestData.test_data_diageoza_sand_sanity import ProjectsSanityData
# from Projects.DIAGEOZA_SAND.Calculations import DIAGEOZASANDCalculations
# from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
#
#
# __author__ = 'yoava'
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
#     @patch('Projects.DIAGEOZA_SAND.Utils.ToolBox.DIAGEOZA_SANDDIAGEOToolBox.get_latest_directory_date_from_cloud',
#            return_value='2018-05-18')
#     @patch('Projects.DIAGEOZA_SAND.Utils.ToolBox.DIAGEOZA_SANDDIAGEOToolBox.save_latest_templates')
#     @patch('Projects.DIAGEOZA_SAND.Utils.ToolBox.DIAGEOZA_SANDDIAGEOToolBox.download_template',
#            return_value=mpa)
#     @patch('Projects.DIAGEOZA_SAND.Utils.ToolBox.DIAGEOZA_SANDDIAGEOToolBox.download_template',
#            return_value=posm)
#     @patch('Projects.DIAGEOZA_SAND.Utils.ToolBox.DIAGEOZA_SANDDIAGEOToolBox.download_template',
#            return_value=products)
#     @seeder.seed(["diageoza_sand_seed"], ProjectsSanityData())
#     def test_diageoza_sand_sanity(self, x, y, json, json2, json3):
#         project_name = ProjectsSanityData.project_name
#         data_provider = KEngineDataProvider(project_name)
#         sessions = ['A54C3D07-F41F-4A49-AC6F-6D590011BDE2']
#         for session in sessions:
#             data_provider.load_session_data(session)
#             output = Output()
#             DIAGEOZASANDCalculations(data_provider, output).run_project_calculations()
#             self._assert_kpi_results_filled()
