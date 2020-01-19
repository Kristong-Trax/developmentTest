from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
# import pandas as pd
from Projects.CCNAYARMX.Utils.KPIToolBox import ToolBox

__author__ = 'krishnat'


class TestCCNAYARMX(TestCase):

    @patch('Projects.CCNAYARMX.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccnayarmx'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = ToolBox(self.data_provider_mock, self.output, MagicMock())
