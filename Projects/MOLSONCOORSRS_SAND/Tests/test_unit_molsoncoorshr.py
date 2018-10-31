
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MOLSONCOORSRS_SAND.Utils.KPIToolBox import MOLSONCOORSRS_SANDToolBox


__author__ = 'sergey'


class MOLSONCOORSRS_SANDTest(TestCase):

    @mock.patch('Projects.MOLSONCOORSRS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'molsoncoorsrs_sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MOLSONCOORSRS_SANDToolBox(self.data_provider_mock, MagicMock())

