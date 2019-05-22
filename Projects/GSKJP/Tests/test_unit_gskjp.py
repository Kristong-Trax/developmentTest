
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.GSKJP.Utils.KPIToolBox import GSKJPToolBox


__author__ = 'limorc'


class TestGSKJP(TestCase):

    @mock.patch('Projects.GSKJP.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskjp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKJPToolBox(self.data_provider_mock, MagicMock())

