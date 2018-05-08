import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Testing.Case import MockingTestCase, TestCase
from mock import MagicMock, mock

from Projects.CCBOTTLERSUS.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox import REDToolBox
from Projects.CCBOTTLERSUS.REDSCORE.Converters import Converters
from Projects.CCBOTTLERSUS.REDSCORE.Checks import Checks

from Projects.CCBOTTLERSUS.REDSCORE.Tests.RedScoreTestData import RedScoreTestData


class RedScoreTests(TestCase):

    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.ProjectConnector')
    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.Common')
    def setUp(self,x,y):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'ccbottlersus'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.test_objects = RedScoreTestData()
        self.tool_box = REDToolBox(self.data_provider_mock, self.output)
        self.checks = Checks(self.data_provider_mock)

    @property
    def import_path(self):
        return 'Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    def test_convert_column_to_scif_filter(self):
        self.assertEquals(Converters.convert_column_to_scif_filter(Const.SCENE_TYPE), 'template_name')

    def test_convert_column_to_store_filter(self):
        self.assertEquals(Converters.convert_column_to_store_filter(Const.STORE_ATT15), 'additional_attribute_15')

    def test_convert_column_to_sos_filter(self):
        self.assertEquals(Converters.convert_sos_filter(Const.MANUFACTURE), 'manufacturer_name')
        self.assertEquals(Converters.convert_sos_filter(Const.BRAND), 'brand_name')



    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.ProjectConnector')
    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.Common')
    def test_get_data_template(self,x,y):
        self.assertIsNotNone(self.tool_box.kpi_template)
        self.assertIsNotNone(self.tool_box.survey_template)

    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.ProjectConnector')
    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.Common')
    def test_check_store_attribute(self,x,y):
        self.tool_box._get_store_attribute_15 = MagicMock()
        self.tool_box._get_store_attribute_15.return_value = 'DP'
        filtered_template = self.tool_box.sku_availability_template[self.tool_box.sku_availability_template[Const.KPI_NAME] =='CR&LT1b']
        self.assertTrue(self.tool_box._check_store_attribute(Const.STORE_ATT15,filtered_template.iloc[4]))

        filtered_template = self.tool_box.sku_availability_template[self.tool_box.sku_availability_template[Const.KPI_NAME] =='CR&LT1b']
        self.assertFalse(self.tool_box._check_store_attribute(Const.STORE_ATT15,filtered_template.iloc[1]))

    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.ProjectConnector')
    @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.Common')
    def test_check_store(self, x, y):

        self.checks = Checks(self.data_provider_mock)

        # sunny day
        self.checks.store_info = self.test_objects.get_good_store_info()
        kpi_details = self.tool_box.kpi_template[(self.tool_box.kpi_template[Const.KPI_GROUP] == Const.RED_SCORE) & (self.tool_box.kpi_template[Const.KPI_NAME] == 'CR&LT1')]
        self.assertTrue(self.checks.check_store(kpi_details.iloc[0]))

        # day rainy
        self.checks = Checks(self.data_provider_mock)
        self.checks.store_info = self.test_objects.get_bad_store_info()
        kpi_details = self.tool_box.kpi_template[(self.tool_box.kpi_template[Const.KPI_GROUP] == Const.RED_SCORE) & (
        self.tool_box.kpi_template[Const.KPI_NAME] == 'CR&LT1')]
        self.assertFalse(self.checks.check_store(kpi_details.iloc[0]))

    # @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.ProjectConnector')
    # @mock.patch('Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox.Common')
    # def test_check_SOS(self, x, y):
    #     filtered_template = self.tool_box.SOS_sheet[self.tool_box.SOS_sheet[Const.KPI_NAME] == 'CR&LT1']
    #     self.assertTrue( self.tool_box._check_SOS(filtered_template.iloc[0]))
    #




