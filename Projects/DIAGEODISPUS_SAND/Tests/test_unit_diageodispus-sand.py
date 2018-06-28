
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.DIAGEODISPUS_SAND.Utils.KPIToolBox import DIAGEODISPUSToolBox


__author__ = 'nissand'


class TestDIAGEODISPUS_SAND(TestCase):

    @mock.patch('Projects.DIAGEODISPUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageodispus-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = DIAGEODISPUSToolBox(self.data_provider_mock, MagicMock())

