#
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Testing.Case import TestCase
# from mock import MagicMock, mock
# import pandas as pd
# from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKToolBox
#
#
# __author__ = 'natalyak'
#
#
# class TestPEPSICOUK(TestCase):
#
#     @mock.patch('Projects.PEPSICOUK.Utils.KPIToolBox.ProjectConnector')
#     def setUp(self, x):
#         Config.init('')
#         self.data_provider_mock = MagicMock()
#         self.data_provider_mock.project_name = 'pepsicouk'
#         self.data_provider_mock.rds_conn = MagicMock()
#         self.output = MagicMock()
#         self.tool_box = PEPSICOUKToolBox(self.data_provider_mock, MagicMock())

from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from Trax.Data.Testing.SeedNew import Seeder
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from mock import MagicMock
from Projects.PEPSICOUK.Utils.KPIToolBox import PEPSICOUKToolBox
from Projects.CCBZA_SAND.Tests.data_test_unit_ccbza_sand import DataTestUnitCCBZA_SAND, DataScores, SCIFDataTestCCBZA_SAND, MatchProdSceneDataTestCCBZA_SAND
from Trax.Algo.Calculations.Core.DataProvider import Output
from mock import patch
import os
import pandas as pd
# from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
#     SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, TARGET, SKU, POS, OTHER, MAX_SCORE
#
# __author__ = 'natalyak'
#
# class TestCCBZA_SAND(MockingTestCase):
#     seeder = Seeder()
#
#     @property
#     def import_path(self):
#         return 'Projects.PEPSICOUK.Utils.KPIToolBox'
