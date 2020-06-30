# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
# from Projects.STRAUSSFRITOLAYIL.Tests.Data.data_test_straussfritolayil_sanity import ProjectsSanityData
# from Trax.Apps.Services.KEngine.Handlers.UnifiedHandler import KEngineUnifiedHandler
# from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
# from Projects.STRAUSSFRITOLAYIL.Tests.Data.kpi_results import STRAUSSFRITOLAYILKpiResults
# from mock import MagicMock
#
# __author__ = 'ilays'
#
#
# class TestKEnginePsCode(PsSanityTestsFuncs):
#
#     def add_mocks(self):
#         return
#
#     @PsSanityTestsFuncs.seeder.seed(["straussfritolayil_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
#     def test_straussfritolayil_sanity(self):
#         self.add_mocks()
#         project_name = ProjectsSanityData.project_name
#         sessions = [{'session_uid': '626d0d90-9103-469d-8ab0-3d428054f664', 'session_id': 22789}]
#         kpi_results = STRAUSSFRITOLAYILKpiResults().get_kpi_results()
#         for session in sessions:
#             message_session = {'event_name': 'SESSION_PROCESSED', 'timestamp': '', 'project_name': project_name,
#                                'session_uid': session['session_uid'], 'session_id': session['session_id'],
#                                'scene_ids': [], 'scene_uids': [], 'number_of_scenes': 1,
#                                'attributes': {'ApproximateReceiveCount': 1}, 'wave_type': 'primary', 'wave_uid': ''}
#             kenigineUnified = KEngineUnifiedHandler()
#             kenigineUnified._process_message(message_session, None, MagicMock(), None)
#         self._assert_test_results_matches_reality(kpi_results)
#         # self._assert_old_tables_kpi_results_filled()
#         # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
#         # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
