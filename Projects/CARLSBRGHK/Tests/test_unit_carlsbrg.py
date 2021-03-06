from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
from Projects.CARLSBRGHK.Utils.KPIToolBox import CARLSBERGToolBox


__author__ = 'nidhin'


class TestCARLSBERG(TestCase):

    @patch('Projects.CARLSBRGHK.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'carlsberg'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CARLSBERGToolBox(self.data_provider_mock, MagicMock())

