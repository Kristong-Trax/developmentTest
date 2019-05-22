
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.GSKAU.Utils.KPIToolBox import GSKAUToolBox


__author__ = 'limorc'


class TestGSKAU(TestCase):

    @mock.patch('Projects.GSKAU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKAUToolBox(self.data_provider_mock, MagicMock())

