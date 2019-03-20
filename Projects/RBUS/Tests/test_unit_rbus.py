from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import MagicMock
import Projects.RBUS_SAND.Tests.test_data as Data
from Projects.RBUS_SAND.Utils.KPIToolBox import RBUSToolBox

__author__ = 'yoava'


class TestRbusSand(MockingTestCase):

    def setUp(self):
        Config.init()
        self.mock_object('PSProjectConnector', path='Projects.RBUS_SAND.Utils.KPIToolBox')
        self.mock_object('common_old', path='Projects.RBUS_SAND.Utils.KPIToolBox')
        self.mock_object('Common', path='Projects.RBUS_SAND.Utils.KPIToolBox')
        self.mock_object('PsDataProvider', path='Projects.RBUS_SAND.Utils.KPIToolBox')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'rbus-sand'

        self.output = Output()

        self.data = Data

        self.tool_box = RBUSToolBox(self.data_provider_mock, self.output)

    def tearDown(self):
        pass

    def test_df_head(self):
        df = self.data.get_scene_data_head()
        self.assertEquals(len(df.loc[df.template_name == 'ADDITIONAL AMBIENT PLACEMENT'].scene_fk.unique()),
                          self.tool_box.get_scene_count(df, 'ADDITIONAL AMBIENT PLACEMENT'))

    def test_num_of_scene_types_ambient(self):
        df = self.data.get_scene_data_complete()
        self.assertEquals(len(df.loc[df.template_name == 'ADDITIONAL AMBIENT PLACEMENT'].scene_fk.unique()),
                          self.tool_box.get_scene_count(df, 'ADDITIONAL AMBIENT PLACEMENT'))

    def test_num_of_scene_types_main_placement(self):
        df = self.data.get_scene_data_complete()
        self.assertEquals(len(df.loc[df.template_name == 'MAIN PLACEMENT'].scene_fk.unique()),
                          self.tool_box.get_scene_count(df, 'MAIN PLACEMENT'))

    def test_num_of_scene_types_non_exits(self):
        df = self.data.get_scene_data_complete()
        self.assertEquals(len(df.loc[df.template_name == 'ABC'].scene_fk.unique()),
                          self.tool_box.get_scene_count(df, 'ABC'))

    def test_get_atomic_kpi(self):
        kpi1 = 'K004'
        kpi2 = 'K005'
        df = self.data.get_atomic_fk()
        self.assertLess(df[df['atomic_kpi_name'] == kpi1]['atomic_kpi_fk'].values[0],
                        df[df['atomic_kpi_name'] == kpi2]['atomic_kpi_fk'].values[0])

