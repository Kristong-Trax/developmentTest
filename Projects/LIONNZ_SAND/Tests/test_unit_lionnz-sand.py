
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
from Projects.LIONNZ_SAND.Utils.KPIToolBox import LIONNZToolBox


__author__ = 'nidhin'


class TestLIONNZ_SAND(TestCase):

    @patch('Projects.LIONNZ_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'lionnz-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = LIONNZToolBox(self.data_provider_mock, MagicMock())

