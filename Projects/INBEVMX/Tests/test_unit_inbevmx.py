
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.INBEVMX.Utils.KPIToolBox import INBEVMXToolBox


__author__ = 'ilays'


class TestINBEVMX_SAND(TestCase):

    @mock.patch('Projects.INBEVMX.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'inbevmx'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = INBEVMXToolBox(self.data_provider_mock, MagicMock())

