
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
# import pandas as pd
from Projects.NESTLEBEVUS.Utils.KPIToolBox import ToolBox


__author__ = 'krishnat'


class TestNESTLEBEVUS(TestCase):

    @mock.patch('Projects.NESTLEBEVUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'nestlebevus'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = ToolBox(self.data_provider_mock, MagicMock())