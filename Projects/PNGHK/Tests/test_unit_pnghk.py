
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.PNGHK.Utils.KPIToolBox import PNGHKToolBox


__author__ = 'nidhinb'


class TestPNGHK(TestCase):

    @mock.patch('Projects.PNGHK.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'pnghk'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = PNGHKToolBox(self.data_provider_mock, MagicMock())
