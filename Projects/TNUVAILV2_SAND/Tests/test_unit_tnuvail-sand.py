
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.TNUVAILV2_SAND.Utils.KPIToolBox import TNUVAILToolBox


__author__ = 'idanr'


class TestTNUVAIL_SAND(TestCase):

    @mock.patch('Projects.TNUVAILV2_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'tnuvail-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = TNUVAILToolBox(self.data_provider_mock, MagicMock())

