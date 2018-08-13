
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCBOTTLERSUS_SAND.REDSCORE.KPIToolBox import CCBOTTLERSUSToolBox


__author__ = 'Elyashiv'


class TestCCBOTTLERSUS_SAND(TestCase):

    @mock.patch('Projects.CCBOTTLERSUS_SAND.REDSCORE.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccbottlersus-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCBOTTLERSUSToolBox(self.data_provider_mock, MagicMock())

