
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SANOFIKZ_SAND.Utils.KPIToolBox import SANOFIKZToolBox


__author__ = 'limorc'


class TestSANOFIKZ_SAND(TestCase):

    @mock.patch('Projects.SANOFIKZ_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'sanofikz-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SANOFIKZToolBox(self.data_provider_mock, MagicMock())

