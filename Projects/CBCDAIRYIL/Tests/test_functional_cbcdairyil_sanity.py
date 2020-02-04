

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.CBCDAIRYIL.Tests.Data.data_test_cbcdairyil_sanity import ProjectsSanityData
from Projects.CBCDAIRYIL.Calculations import Calculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.CBCDAIRYIL.Tests.Data.kpi_results import CBCDAIRYILKpiResults
# import os
# import json

__author__ = 'ilays'


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

    @PsSanityTestsFuncs.seeder.seed(["cbcdairyil_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_cbcdairyil_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'bd2f968c-8c47-4565-8422-82b43729e6a4': []}
        kpi_results = CBCDAIRYILKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        self._assert_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
