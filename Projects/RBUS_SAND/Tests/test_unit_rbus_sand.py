from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.RBUS_SAND.Tests.test_data as Data
from Projects.RBUS_SAND.Utils.KPIToolBox import RBUSToolBox

__author__ = 'yoava'


class TestRbusSand(TestCase):

    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.PSProjectConnector')
    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.common_old')
    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.PsDataProvider')
    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.Common')
    def setUp(self, x, y, z, a):
        Config.init()
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'rbus-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.data = Data
        self.tool_box = RBUSToolBox(self.data_provider_mock, self.output)


class TestPlacementCount(TestRbusSand):

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


class TestShelfOccupation(TestRbusSand):
    def test_get_atomic_kpi(self):
        kpi1 = 'K004'
        kpi2 = 'K005'
        df = self.data.get_atomic_fk()
        self.assertLess(df[df['atomic_kpi_name'] == kpi1]['atomic_kpi_fk'].values[0],
                        df[df['atomic_kpi_name'] == kpi2]['atomic_kpi_fk'].values[0])


if __name__ == '__main__':
    placement_count_tester = TestPlacementCount()
    placement_count_tester.test_df_head()
    placement_count_tester.test_num_of_scene_types_ambient()
    placement_count_tester.test_num_of_scene_types_main_placement()
    placement_count_tester.test_num_of_scene_types_non_exits()

    shelf_occupation_tester = TestShelfOccupation()
    shelf_occupation_tester.test_get_atomic_kpi()
