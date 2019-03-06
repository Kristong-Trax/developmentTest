
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.NESTLEBAKINGUS.Utils.KPIToolBox import NESTLEBAKINGUSToolBox


__author__ = 'huntery'


class TestNESTLEBAKINGUS(TestCase):

    @mock.patch('Projects.NESTLEBAKINGUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'nestlebakingus'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = NESTLEBAKINGUSToolBox(self.data_provider_mock, MagicMock())

