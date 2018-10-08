
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SOLARBR_SAND.Utils.KPIToolBox import SOLARBRToolBox


__author__ = 'nicolaske'


class TestSOLARBR_SAND(TestCase):

    @mock.patch('Projects.SOLARBR_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'solarbr-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SOLARBRToolBox(self.data_provider_mock, MagicMock())

