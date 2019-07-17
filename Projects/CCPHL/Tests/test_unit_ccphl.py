
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCPHL.Utils.KPIToolBox import CCPHLToolBox


__author__ = 'sathiyanarayanan'


class TestCCPHL(TestCase):

    @mock.patch('Projects.CCPHL.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccphl'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCPHLToolBox(self.data_provider_mock, MagicMock())

