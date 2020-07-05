

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.RISPARKWINEDE_SAND.Tests.Data.data_test_risparkwinede_sand_sanity import ProjectsSanityData
from Projects.RISPARKWINEDE_SAND.Calculations import Calculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.RISPARKWINEDE_SAND.Tests.Data.kpi_results import RISPARKWINEDE_SANDKpiResults
# import os
# import json

__author__ = 'limorc'


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

    @PsSanityTestsFuncs.seeder.seed(["risparkwinede_sand_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_risparkwinede_sand_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'aedf0226-9694-4da5-9329-6f6cc4bee18a': []}
        kpi_results = RISPARKWINEDE_SANDKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
        self._assert_test_results_matches_reality(kpi_results)

