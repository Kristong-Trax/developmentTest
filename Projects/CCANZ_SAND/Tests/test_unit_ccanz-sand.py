
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCANZ_SAND.Utils.KPIToolBox import CCANZToolBox


__author__ = 'limorc'


class TestCCANZ_SAND(TestCase):

    @mock.patch('Projects.CCANZ_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccanz-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCANZToolBox(self.data_provider_mock, MagicMock())

