
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MOLSONCOORSHR.Utils.KPIToolBox import MOLSONCOORSHRToolBox


__author__ = 'sergey'


class TestMOLSONCOORSHR(TestCase):

    @mock.patch('Projects.MOLSONCOORSHR.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'molsoncoorshr'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MOLSONCOORSHRToolBox(self.data_provider_mock, MagicMock())

