
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SINOPAC_LOCAL.Utils.KPIToolBox import SinoPacificToolBox


__author__ = 'nidhin'


class TestLIONJP(TestCase):

    @mock.patch('Projects.SINOPAC_LOCAL.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'lionjp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SinoPacificToolBox(self.data_provider_mock, MagicMock())

