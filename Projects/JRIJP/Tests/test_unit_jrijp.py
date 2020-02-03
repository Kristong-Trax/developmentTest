
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
from Projects.JRIJP.Utils.KPIToolBox import JRIJPToolBox


__author__ = 'nidhin'


class TestJRIJP(TestCase):

    @patch('Projects.JRIJP.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'jrijp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = JRIJPToolBox(self.data_provider_mock, MagicMock())

