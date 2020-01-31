

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Projects.CCRU.Tests.Data.data_test_ccru_sanity import ProjectsSanityData
from Projects.CCRU.Calculations import CCRUCalculations
from DevloperTools.SanityTests.PsSanityTests import PsSanityTestsFuncs
from Projects.CCRU.Tests.Data.kpi_results import CCRUKpiResults
import json

__author__ = 'sergey'

EQUIPMENT_TARGETS_FILE = '461496'  # by store_fk of 'F26E2E6B-D12B-415C-AC0C-CAB929BEFC9F'
SESSION_LIST = {'F26E2E6B-D12B-415C-AC0C-CAB929BEFC9F': [],
                '3b8a8039-2c79-436d-b42f-c72f4ce3b183': []}


class TestKEnginePsCode(PsSanityTestsFuncs):

    def add_mocks(self):
        with open('./Data/{}'.format(EQUIPMENT_TARGETS_FILE), 'rb') as f:
            self.mock_object('get_equipment_targets',
                             path='Projects.CCRU.Utils.ToolBox.CCRUKPIToolBox')\
                .return_value = json.load(f)
        return

    @PsSanityTestsFuncs.seeder.seed(["ccru_seed", "mongodb_products_and_brands_seed"], ProjectsSanityData())
    def test_ccru_sanity(self):
        self.add_mocks()
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = SESSION_LIST
        kpi_results = CCRUKpiResults().get_kpi_results()
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            CCRUCalculations(data_provider, output).run_project_calculations()
            # for scene in sessions[session]:
            # data_provider.load_scene_data(str(session), scene_id=scene)
            # SceneCalculations(data_provider).calculate_kpis()
        self._assert_test_results_matches_reality(kpi_results)
        # self._assert_old_tables_kpi_results_filled()
        # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
        # self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
