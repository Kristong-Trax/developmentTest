import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
from Projects.DIAGEOPT.Tests.Data.test_data_diageopt import ProjectsSanityData
from Projects.DIAGEOPT.Utils.KPIToolBox import DIAGEOPTToolBox

__author__ = 'yoava'


class TestDiageopt(TestMockingFunctionalCase):
    seeder = Seeder()

    def set_up(self):
        super(TestDiageopt, self).set_up()
        self.project_name = ProjectsSanityData.project_name
        self.output = Output()
        self.session_uid = '9F3E857F-E238-4380-A16C-E23E909E1DD1'


    @property
    def import_path(self):
        return 'Projects.DIAGEOPT.Utils.KPIToolBox'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    @seeder.seed(["diageopt_seed"], ProjectsSanityData())
    def test_get_kpi_static_data(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOPTToolBox(data_provider, self.output)
        result = tool_box.get_kpi_static_data()
        self.assertEquals(type(result), pd.DataFrame)

    @seeder.seed(["diageopt_seed"], ProjectsSanityData())
    def test_get_business_unit(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOPTToolBox(data_provider, self.output)
        result = tool_box.get_business_unit()
        self.assertEquals(type(result), str)

    @seeder.seed(["diageopt_seed"], ProjectsSanityData())
    def test_get_match_display(self):
        data_provider = KEngineDataProvider(self.project_name)
        data_provider.load_session_data(self.session_uid)
        tool_box = DIAGEOPTToolBox(data_provider, self.output)
        result = tool_box.get_match_display()
        self.assertEquals(type(result), pd.DataFrame)

