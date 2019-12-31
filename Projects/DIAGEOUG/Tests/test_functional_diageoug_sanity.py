
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.DIAGEOUG.Tests.Data.test_data_diageoug_sanity import ProjectsSanityData
from Projects.DIAGEOUG.Calculations import DIAGEOUGCalculations

from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.DIAGEOUG.Tests.Data.kpi_results import DIAGEOUGKpiResults

__author__ = 'ilays'


class TestKEnginePsCode(PsSanityTestsFuncs):

    @PsSanityTestsFuncs.seeder(["diageoug_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_diageoug_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'f9d6b8a5-7964-4ef5-afe4-8580df97f57c': []}
        kpi_results = DIAGEOUGKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            DIAGEOUGCalculations(data_provider, output).run_project_calculations()
            self._assert_test_results_matches_reality(kpi_results)
            # self._assert_old_tables_kpi_results_filled(distinct_kpis_num=None)
            # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
            # for scene in sessions[session]:
            #     data_provider.load_scene_data(str(session), scene_id=scene)
            #     SceneCalculations(data_provider).calculate_kpis()
            #     self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
