
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.TEST3.Utils.KPIToolBox import TEST3ToolBox


__author__ = 'ilays'


class TestTEST3(TestCase):

    @mock.patch('Projects.TEST3.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'test3'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = TEST3ToolBox(self.data_provider_mock, MagicMock())

