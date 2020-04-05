
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock
import pandas as pd
from Projects.CCKR_SAND.Utils.KPIToolBox import CCKR_SANDToolBox


__author__ = 'limorc'


class TestCCKR_SAND(TestCase):

    @mock.patch('Projects.CCKR_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'cckr-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = CCKR_SANDToolBox(self.data_provider_mock, MagicMock())

