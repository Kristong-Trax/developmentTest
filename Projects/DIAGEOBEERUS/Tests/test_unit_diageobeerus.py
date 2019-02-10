
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.DIAGEOBEERUS.Utils.KPIToolBox import DIAGEOBEERUSToolBox


__author__ = 'huntery'


class TestDIAGEOBEERUS(TestCase):

    @mock.patch('Projects.DIAGEOBEERUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageobeerus'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = DIAGEOBEERUSToolBox(self.data_provider_mock, MagicMock())

