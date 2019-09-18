
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SANOFIJP.Utils.KPIToolBox import SanofiJPToolBox


__author__ = 'nidhin'


class TestTWEGAU(TestCase):

    @mock.patch('Projects.TWEGAU.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'twegau'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SanofiJPToolBox(self.data_provider_mock, MagicMock())

