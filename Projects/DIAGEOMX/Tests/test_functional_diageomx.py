import os
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.DIAGEOMX.Tests.Data.test_data_diageomx import ProjectsSanityData
from Projects.DIAGEOMX.Utils.KPIToolBox import DIAGEOMXToolBox
import pandas as pd

__author__ = 'yoava'


class TestDiageomx(TestMockingFunctionalCase):
    seeder = Seeder()

    def set_up(self):
        self.project_name = ProjectsSanityData.project_name
        self.output = Output()
        self.session_uid = '8e5c105e-5457-4c50-a934-7324706c1c29'

    def tear_down(self):
        pass

    @property
    def import_path(self):
        return 'Projects.DIAGEOMX.Utils.KPIToolBox'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_get_kpi_static_data(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOMXToolBox(data_provider, self.output)
        result = tool_box.get_kpi_static_data()
        self.assertEquals(type(result), pd.DataFrame)

    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_get_business_unit(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOMXToolBox(data_provider, self.output)
        result = tool_box.get_business_unit()
        self.assertEquals(type(result), str)

    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_get_match_display(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOMXToolBox(data_provider, self.output)
        result = tool_box.get_match_display()
        self.assertEquals(type(result), pd.DataFrame)

    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_main_calculation(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOMXToolBox(data_provider, self.output)
        result = tool_box.main_calculation("main_calculation")
        self.assertIsNone(result)

    #_get_direction_for_relative_position
    @seeder.seed(["diageomx_seed"], ProjectsSanityData())
    def test_get_direction_for_relative_position(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOMXToolBox(data_provider, self.output)
        result = tool_box._get_direction_for_relative_position(60)
        self.assertIsInstance(result, int)



