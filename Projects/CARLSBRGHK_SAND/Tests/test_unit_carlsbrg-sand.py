from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
from Projects.CARLSBRGHK_SAND.Utils.KPIToolBox import CARLSBERGToolBox


__author__ = 'nidhin'


class TestCARLSBERG_SAND(TestCase):

    @patch('Projects.CARLSBRGHK_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'carlsberg-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CARLSBERGToolBox(self.data_provider_mock, MagicMock())

