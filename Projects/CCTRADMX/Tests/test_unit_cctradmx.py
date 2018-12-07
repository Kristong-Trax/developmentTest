
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCTRADMX.Utils.KPIToolBox import CCTRADMXToolBox


__author__ = 'ilays'


class TestCCTRADMX(TestCase):

    @mock.patch('Projects.CCTRADMX.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'cctradmx'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCTRADMXToolBox(self.data_provider_mock, MagicMock())

