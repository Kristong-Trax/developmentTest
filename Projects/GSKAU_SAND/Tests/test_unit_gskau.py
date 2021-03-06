
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.GSKAU_SAND.Utils.KPIToolBox import GSKAU_SANDToolBox


__author__ = 'limorc'


class TestGSKAU_SAND(TestCase):

    @mock.patch('Projects.GSKAU_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKAU_SANDToolBox(self.data_provider_mock, MagicMock())

