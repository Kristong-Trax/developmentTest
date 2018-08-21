
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.CCBZA.Utils.KPIToolBox import CCBZAToolBox


# __author__ = 'natalyak'
#
#
# class TestCCBZA(TestCase):
#
#     @mock.patch('Projects.CCBZA.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'ccbza'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = CCBZAToolBox(self.data_provider_mock, MagicMock())
#
