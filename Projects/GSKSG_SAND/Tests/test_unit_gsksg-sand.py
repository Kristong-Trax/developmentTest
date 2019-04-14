
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.GSKSG_SAND.Utils.KPIToolBox import GSKSGToolBox


__author__ = 'urid'


class TestGSKSG_SAND(TestCase):

    @mock.patch('Projects.GSKSG_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gsksg-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())

