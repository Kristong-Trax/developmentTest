
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCAAU.Utils.KPIToolBox import CCAAUToolBox


__author__ = 'limorc'


class TestCCAAU(TestCase):

    @mock.patch('Projects.CCAAU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccaau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCAAUToolBox(self.data_provider_mock, MagicMock())

