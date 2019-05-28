
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKSG_SAND2.Utils.KPIToolBox import GSKSGToolBox


__author__ = 'jasmine'


class TestGSKSG(TestCase):

    @Mock.patch('Projects.GSKSG.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gsksg'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())

