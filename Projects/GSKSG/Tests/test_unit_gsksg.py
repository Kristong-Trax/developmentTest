
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKSG.Utils.KPIToolBox import GSKSGToolBox
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
import os


__author__ = 'limorc'


class TestGSKSG(MockingTestCase):

    @property
    def import_path(self):
        return 'Projects.GSKSG.Utils.KPIToolBox'

    def set_up(self):
        super(TestGSKSG, self).set_up()
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.project_connector_mock = self.mock_ps_data_provider_project_connector()
        self.common = self.mock_common()
        self.output = MagicMock()
        self.rds_conn = MagicMock()
        # self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())



        # self.data_provider_mock = MagicMock()
        # self.data_provider_mock.project_name = 'Test_Project_1'
        # self.data_provider_mock.rds_conn = MagicMock()
        # self.data_provider_mock.project_connector = self.mock_ps_data_provider_project_connector()
        # self.project_connector_mock = MagicMock()
        # self.common = self.mock_common()
        # self.output = MagicMock()
        #
        # # self.scif = self.get_scif()
        # # self.match_product_in_scene=self.get_match_product()

        self.gsk_generator = MagicMock()
        self.gsk_generator.tool_box = MagicMock()
        self.gsk_generator.tool_box.sub_category = self.mock_category_data_provider()
        self.gsk_generator.tool_box.data_provider = MagicMock()
        self.gsk_generator.tool_box.output = MagicMock()
        self.gsk_generator.tool_box.data_provider.project_connector_mock = self.mock_project_connector()

        self.gsk_generator.tool_box.data_provider.rds_conn = MagicMock()
        self.gsk_generator.tool_box.data_provider.project_name = 'Test_Project_1'
        self.gsk_generator.tool_box.assortment = self.mock_assortment()
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())


    def mock_category_data_provider(self):
        cat_data = self.mock_object('PsDataProvider.get_sub_category', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        cat_data.return_value = ''
        return cat_data.return_value

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')


    def mock_assortment(self):
        self.gsk_generator.tool_box.data_provider = MagicMock()
        self.gsk_generator.tool_box.output = MagicMock()
        return self.gsk_generator.tool_box.mock_object('Assortment')
        # assortment_provider = self.mock_object('PSAssortmentDataProvider', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        # cat_data = self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')
        # cat_data.return_value = ''
        # return MagicMock()


    def mock_ps_data_provider_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    def mock_common(self):
        return self.mock_object('Common')

    # def get_scif(self):
    #     return self.mock_object('scif')
    #
    # def get_match_product(self):
    #     return self.mock_object('match_product_in_scene')


    def test_Availbaility(self):

        df = {'manu': [1, 2], 'product_fk': [3, 4]}
        filters_num = {}
        filters_den = {}
        sos_policy = 'width_mm_advance'
        self.assertEquals(self.gsk_generator.tool_box.calculate_sos(df, filters_num, filters_den, sos_policy))