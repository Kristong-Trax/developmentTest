
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.TSINGTAOBEERCN_SAND.Utils.KPIToolBox import TSINGTAOBEERCNToolBox


__author__ = 'ilays'


class TestTSINGTAOBEERCN_SAND(TestCase):

    @mock.patch('Projects.TSINGTAOBEERCN_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'tsingtaobeercn-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = TSINGTAOBEERCNToolBox(self.data_provider_mock, MagicMock())

