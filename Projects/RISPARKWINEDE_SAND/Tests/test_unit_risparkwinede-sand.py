#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# # import pandas as pd
# from Projects.RISPARKWINEDE_SAND.Utils.KPIToolBox import ToolBox
#
#
# __author__ = 'limorc'
#
#
# class TestRISPARKWINEDE_SAND(TestCase):
#
#     @mock.patch('Projects.RISPARKWINEDE_SAND.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'risparkwinede-sand'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = ToolBox(self.data_provider_mock, MagicMock())
