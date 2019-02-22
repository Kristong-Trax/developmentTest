
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.MILLERCOORS_SAND.Utils.KPIToolBox import MILLERCOORSToolBox


__author__ = 'huntery'


class TestMILLERCOORS_SAND(TestCase):

    @mock.patch('Projects.MILLERCOORS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'millercoors-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = MILLERCOORSToolBox(self.data_provider_mock, MagicMock())

