
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCKR.Utils.KPIToolBox import CCKRToolBox


__author__ = 'limorc'


class TestCCKR(TestCase):

    @mock.patch('Projects.CCKR.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'cckr'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCKRToolBox(self.data_provider_mock, MagicMock())

