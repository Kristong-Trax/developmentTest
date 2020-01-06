

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.CCZA.Tests.Data.data_test_ccza_sanity import ProjectsSanityData
from Projects.CCZA.Calculations import CCZACalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
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

    @PsSanityTestsFuncs.seeder.seed(["ccza_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_ccza_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {u'2cf51a61-e742-496c-8b68-c57dc29c9dac': []}
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            CCZACalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        # self._assert_test_results_matches_reality(kpi_results)
        self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
