
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
from Projects.GSKRU.Utils.KPIToolBox import GSKRUToolBox


__author__ = 'sergey'


class TestGSKRU(TestCase):

    @patch('Projects.GSKRU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskru'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKRUToolBox(self.data_provider_mock, MagicMock())

