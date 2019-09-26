
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
# import pandas as pd
from Projects.DIAGEONG.Utils.KPIToolBox import DIAGEONGToolBox


__author__ = 'michaela'


class TestDIAGEONG(TestCase):

    @mock.patch('Projects.DIAGEONG.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageong'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = DIAGEONGToolBox(self.data_provider_mock, MagicMock())
