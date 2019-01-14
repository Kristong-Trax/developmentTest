
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.NESTLEBR.Utils.KPIToolBox import NESTLEBRToolBox


__author__ = 'huntery'


class TestNESTLEBR(TestCase):

    @mock.patch('Projects.NESTLEBR.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'nestlebr'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = NESTLEBRToolBox(self.data_provider_mock, MagicMock())

