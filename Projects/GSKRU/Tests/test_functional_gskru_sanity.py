

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.GSKRU.Tests.Data.data_test_gskru_sanity import ProjectsSanityData
from Projects.GSKRU.Calculations import Calculations as GSKRUCalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.GSKRU.Tests.Data.kpi_results import GSKRUKpiResults
# import os
# import json

from Trax.Utils.Testing.Case import skip

__author__ = 'sergey'


class TestKEnginePsCode(PsSanityTestsFuncs):

    def add_mocks(self):
        # with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Relative Position.txt'),
        #           'rb') as f:
        #     relative_position_template = json.load(f)
        # self.mock_object('save_latest_templates',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        # self.mock_object('download_template',
        #                  path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler').return_value = \In
        #     relative_position_template
        return

    @skip('fix later')
    @PsSanityTestsFuncs.seeder.seed(["gskru_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_gskau_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'E7D1405E-7128-4FDC-B38A-298AA531899A': []}
        kpi_results = GSKRUKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            GSKRUCalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        self._assert_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
