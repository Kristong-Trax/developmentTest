
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCPH.Utils.KPIToolBox import CCPHToolBox


__author__ = 'satya'


class TestCCPH(TestCase):

    @mock.patch('Projects.CCPH.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccph'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCPHToolBox(self.data_provider_mock, MagicMock())

