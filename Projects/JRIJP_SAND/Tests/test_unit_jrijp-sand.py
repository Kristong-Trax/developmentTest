
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.JRIJP_SAND.Utils.KPIToolBox import JRIJPToolBox


__author__ = 'nidhin'


class TestJRIJP_SAND(TestCase):

    @mock.patch('Projects.JRIJP_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'jrijp-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = JRIJPToolBox(self.data_provider_mock, MagicMock())
