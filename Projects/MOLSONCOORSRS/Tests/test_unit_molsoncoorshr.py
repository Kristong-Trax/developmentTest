
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MOLSONCOORSRS.Utils.KPIToolBox import MOLSONCOORSRSToolBox


__author__ = 'sergey'


class MOLSONCOORSRSTest(TestCase):

    @mock.patch('Projects.MOLSONCOORSRS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'molsoncoorsrs'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MOLSONCOORSRSToolBox(self.data_provider_mock, MagicMock())

