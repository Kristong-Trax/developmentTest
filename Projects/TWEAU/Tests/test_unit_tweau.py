
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.TWEAU.Utils.KPIToolBox import TWEAUToolBox


__author__ = 'sathiyanarayanan'


class TestTWEAU(TestCase):

    @mock.patch('Projects.TWEAU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'tweau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = TWEAUToolBox(self.data_provider_mock, MagicMock())

