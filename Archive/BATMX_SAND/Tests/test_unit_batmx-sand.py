
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.BATMX_SAND.Utils.KPIToolBox import BATMXToolBox


__author__ = 'elyashiv'


class TestBATMX_SAND(TestCase):

    @mock.patch('Projects.BATMX_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'batmx-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = BATMXToolBox(self.data_provider_mock, MagicMock())

