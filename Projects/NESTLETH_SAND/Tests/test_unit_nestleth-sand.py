
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.NESTLETH_SAND.Utils.KPIToolBox import NESTLETHToolBox


__author__ = 'limorc'


class TestNESTLETH_SAND(TestCase):

    @mock.patch('Projects.NESTLETH_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'nestleth-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = NESTLETHToolBox(self.data_provider_mock, MagicMock())

