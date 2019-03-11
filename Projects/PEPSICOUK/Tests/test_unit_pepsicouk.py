
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKToolBox


__author__ = 'natalyak'


class TestPEPSICOUK(TestCase):

    @mock.patch('Projects.PEPSICOUK.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'pepsicouk'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = PEPSICOUKToolBox(self.data_provider_mock, MagicMock())

