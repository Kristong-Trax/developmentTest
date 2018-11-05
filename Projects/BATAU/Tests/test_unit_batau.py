
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.BATAU.Utils.KPIToolBox import BATAUToolBox


__author__ = 'sathiyanarayanan'


class TestBATAU(TestCase):

    @mock.patch('Projects.BATAU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'batau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = BATAUToolBox(self.data_provider_mock, MagicMock())

