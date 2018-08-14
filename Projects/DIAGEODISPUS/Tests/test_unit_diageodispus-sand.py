
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.DIAGEODISPUS.Utils.KPIToolBox import DIAGEODISPUSToolBox


__author__ = 'nissand'


class TestDIAGEODISPUS(TestCase):

    @mock.patch('Projects.DIAGEODISPUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageodispus'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = DIAGEODISPUSToolBox(self.data_provider_mock, MagicMock())

