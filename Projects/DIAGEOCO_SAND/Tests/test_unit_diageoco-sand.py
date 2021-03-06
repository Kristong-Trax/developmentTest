
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.DIAGEOCO_SAND.Utils.KPIToolBox import DIAGEOCOToolBox


__author__ = 'huntery'


class TestDIAGEOCO_SAND(TestCase):

    @mock.patch('Projects.DIAGEOCO_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageoco-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = DIAGEOCOToolBox(self.data_provider_mock, MagicMock())

