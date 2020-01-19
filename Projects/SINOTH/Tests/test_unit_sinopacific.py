
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, patch
import pandas as pd
from Projects.SINOTH.Utils.KPIToolBox import SinoPacificToolBox


__author__ = 'nidhin'


class TestSINOTH(TestCase):

    @patch('Projects.SINOTH.Utils.KPIToolBox.ProjectConnector')
    def setUp(self):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'sinoth'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SinoPacificToolBox(self.data_provider_mock, MagicMock())

