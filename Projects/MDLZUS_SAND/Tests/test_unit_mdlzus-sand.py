
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MDLZUS_SAND.Utils.KPIToolBox import MDLZUSToolBox


__author__ = 'ilays'


class TestMDLZUS_SAND(TestCase):

    @mock.patch('Projects.MDLZUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'mdlzus-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MDLZUSToolBox(self.data_provider_mock, MagicMock())

