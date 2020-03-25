
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
import pandas as pd
from Projects.LIONNZ.Utils.KPIToolBox import LIONNZToolBox


__author__ = 'nidhin'


class TestLIONNZ(TestCase):

    @patch('Projects.LIONNZ.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'lionnz'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = LIONNZToolBox(self.data_provider_mock, MagicMock())

