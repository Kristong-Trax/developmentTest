
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MOLSONCOORSHR_SAND.Utils.KPIToolBox import MOLSONCOORSHR_SANDToolBox


__author__ = 'sergey'


class MOLSONCOORSHR_SANDTest(TestCase):

    @mock.patch('Projects.MOLSONCOORSHR_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'molsoncoorshr_sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MOLSONCOORSHR_SANDToolBox(self.data_provider_mock, MagicMock())

