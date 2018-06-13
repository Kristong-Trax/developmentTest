from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
#import Projects.PERFETTICN_SAND.Tests.test_data as Data
from  Projects.PERFETTICN_SAND.Utils.KPIToolBox import PERFETTICNToolBox

__author__ = 'limorc'


class TestPerffetiSand(TestCase):

    @mock.patch('Projects.RBUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init()
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'perfetticn-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
       # self.data = Data
        self.tool_box = PERFETTICNToolBox(self.data_provider_mock, self.output)

class TestDisplayOrder(TestPerffetiSand):

    def test_display_correct(self):
        df = self.tool_box.display_count()
        return
        # bring info to the scene data






if __name__ == '__main__':
    TestDisplayOrder.test_display_correct()

