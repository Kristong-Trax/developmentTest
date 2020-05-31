
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.LIONJP.Utils.KPIToolBox import LIONJPToolBox


__author__ = 'nidhin'


class TestLIONJP(TestCase):

    @mock.patch('Projects.LIONJP.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'lionjp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = LIONJPToolBox(self.data_provider_mock, MagicMock())

