

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.DIAGEORU.Tests.Data.data_test_diageoru_sanity import ProjectsSanityData
from Projects.DIAGEORU.Calculations import DIAGEORUDIAGEORUCalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.DIAGEORU.Tests.Data.kpi_results import DIAGEORUKpiResults
import os
import json

__author__ = 'ilays'


class TestKEnginePsCode(PsSanityTestsFuncs):

    def add_mocks(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Brand Blocking.txt'),
                  'rb') as f:
            brand_blocking_template = json.load(f)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Relative Position.txt'),
                  'rb') as f:
            relative_position_template = json.load(f)
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Vertical Shelf Placement.txt'),
                  'rb') as f:
            vertical_placment_template = json.load(f)
        self.mock_object('save_latest_templates',
                         path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        self.mock_object('download_template',
                         path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler').side_effect = \
            brand_blocking_template, relative_position_template, vertical_placment_template

    @PsSanityTestsFuncs.seeder.seed(["diageoru_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_diageoru_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = {'dafebfb1-b4bf-41b7-b419-e901b4e59c32': []}
        kpi_results = DIAGEORUKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            DIAGEORUDIAGEORUCalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        self._assert_DIAGEO_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
