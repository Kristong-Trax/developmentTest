
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.PEPSICORU.Utils.KPIToolBox import PEPSICORUToolBox


__author__ = 'idanr'


class TestPEPSICORU(TestCase):

    @mock.patch('Projects.PEPSICORU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'pepsicoru'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = PEPSICORUToolBox(self.data_provider_mock, MagicMock())

