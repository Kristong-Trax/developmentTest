import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.DIAGEOTW.Calculations import DIAGEOTWCalculations
from Projects.DIAGEOTW.Tests.Data.test_data_diageotw import ProjectsSanityData
from Projects.DIAGEOTW.Utils.KPIToolBox import DIAGEOTWToolBox

__author__ = 'yoava'


class TestDiageotw(TestMockingFunctionalCase):
    seeder = Seeder()

    def set_up(self):
        super(TestDiageotw, self).set_up()
        self.project_name = ProjectsSanityData.project_name
        self.output = Output()
        self.mock_object('save_latest_templates', path='KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox')
        self.session_uid = 'E9C9D024-5CD2-46F1-A759-2E527207B161'

    @property
    def import_path(self):
        return 'Projects.DIAGEOTW.Utils.KPIToolBox'

    @seeder.seed(["diageotw_seed"], ProjectsSanityData())
    def test_diageotw_sanity(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        DIAGEOTWCalculations(data_provider, self.output).run_project_calculations()

    @seeder.seed(["diageotw_seed"], ProjectsSanityData())
    def test_get_kpi_static_data_return_type(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOTWToolBox(data_provider, self.output)
        result = tool_box.get_kpi_static_data()
        self.assertIsNotNone(result)
        expected_result = pd.DataFrame
        self.assertIsInstance(result, expected_result)

    @seeder.seed(["diageotw_seed"], ProjectsSanityData())
    def test_get_match_display_return_type(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOTWToolBox(data_provider, self.output)
        result = tool_box.get_kpi_static_data()
        self.assertIsNotNone(result)
        expected_result = pd.DataFrame
        self.assertIsInstance(result, expected_result)

