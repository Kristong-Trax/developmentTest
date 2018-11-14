
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MARSUAE.Utils.KPIToolBox import MARSUAEToolBox


__author__ = 'israels'


class TestMARSUAE(TestCase):

    @mock.patch('Projects.MARSUAE.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'marsuae'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MARSUAEToolBox(self.data_provider_mock, MagicMock())

