

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.JNJUK.Tests.Data.data_test_jnjuk_sanity import ProjectsSanityData
from Projects.JNJUK.Calculations import JNJUKCalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.JNJUK.Tests.Data.kpi_results import JNJUKKpiResults
# import os
# import json

__author__ = 'ilays'


class TestKEnginePsCode(PsSanityTestsFuncs):

    def add_mocks(self):
        # with open(os.path.join('Data', 'Relative Position'), 'rb') as f:
        #     relative_position_template = json.load(f)
        # self.mock_object('save_latest_templates',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        # self.mock_object('download_template',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler').return_value = \
        #     relative_position_template
        return

    @PsSanityTestsFuncs.seeder.seed(["jnjuk_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_jnjuk_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'a31b8f7a-9327-4f8e-8495-6c8bba2b4d0e': []}
        kpi_results = JNJUKKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            JNJUKCalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        # self._assert_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
