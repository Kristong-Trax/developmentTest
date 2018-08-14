#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# import pandas as pd
# from Projects.HEINEKENTW.Utils.KPIToolBox import HEINEKENTWToolBox
#
#
# __author__ = 'limorc'
#
#
# class TestHEINEKENTW(TestCase):
#
#     @mock.patch('Projects.HEINEKENTW.Utils.KPIToolBox.ProjectConnector')
#     @mock.patch('Projects.HEINEKENTW.Utils.KPIToolBox.Common')
#     def setUp(self,x,y):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'heinekentw'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = HEINEKENTWToolBox(self.data_provider_mock, MagicMock())
#
#
#     def test_mock(self):
#         self.assertEquals("true",3,"not good")
#
# if __name__=="__main__":
#     TestHEINEKENTW()
