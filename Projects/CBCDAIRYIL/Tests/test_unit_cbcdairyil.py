
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CBCDAIRYIL.Utils.KPIToolBox import CBCDAIRYILToolBox


__author__ = 'idanr'


class TestCBCDAIRYIL(TestCase):

    @mock.patch('Projects.CBCDAIRYIL.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'cbcdairyil'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CBCDAIRYILToolBox(self.data_provider_mock, MagicMock())

