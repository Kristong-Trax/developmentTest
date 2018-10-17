
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.ALTRIAUS_SAND.Utils.KPIToolBox import ALTRIAUSToolBox


__author__ = 'nicolaske'


class TestALTRIAUS_SAND(TestCase):

    @mock.patch('Projects.ALTRIAUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'altriaus-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = ALTRIAUSToolBox(self.data_provider_mock, MagicMock())

