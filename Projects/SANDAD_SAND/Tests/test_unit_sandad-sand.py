
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.SANDAD_SAND.Utils.KPIToolBox import SANDADToolBox


__author__ = 'sathiyanarayanan'


class TestSANDAD_SAND(TestCase):

    @mock.patch('Projects.SANDAD_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'sandad-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = SANDADToolBox(self.data_provider_mock, MagicMock())

