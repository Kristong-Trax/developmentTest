
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SINGHATH.Utils.KPIToolBox import SINGHATHToolBox


__author__ = 'nidhin'


class TestSINGHATH(TestCase):

    @mock.patch('Projects.SINGHATH.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'singhath'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SINGHATHToolBox(self.data_provider_mock, MagicMock())
