
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.HEINEKENTW_SAND.Utils.KPIToolBox import HEINEKENTWToolBox


__author__ = 'limorc'


class TestHEINEKENTW_SAND(TestCase):

    @mock.patch('Projects.HEINEKENTW_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'heinekentw-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = HEINEKENTWToolBox(self.data_provider_mock, MagicMock())

