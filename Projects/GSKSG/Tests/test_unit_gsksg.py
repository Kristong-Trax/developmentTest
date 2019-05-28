
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKSG.Utils.KPIToolBox import GSKSGToolBox
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
import os
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox


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
        self.output = MagicMock()
        self.rds_conn = MagicMock()
        self.project_connector_mock = self.mock_project_connector()
        self.ps_dataprovider_project_connector_mock = self.mock_ps_data_provider_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.output = MagicMock()
        self.gsk_generator = MagicMock()
        self.gsk_generator.tool_box.sub_category = self.mock_category_data_provider()
        self.mock_various_project_connectors()
        self.mock_ps_assortment_dp()
        self.last_session_uid = self.mock_get_last_session_uid()
        self.last_results = self.mock_get_last_results()
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)
        self.gsk_generator.tool_box = GSKToolBox(self.data_provider_mock, MagicMock(),MagicMock(),self.set_up_template)

    def mock_ps_assortment_dp(self):
        self.mock_object('PSAssortmentDataProvider', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')



    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_category_data_provider(self):
        cat_data = self.mock_object('PsDataProvider.get_sub_category', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        cat_data.return_value = ''
        return cat_data.return_value

    def mock_get_last_session_uid(self):
        cat_data = self.mock_object('PsDataProvider.get_last_session', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        cat_data.return_value = ''
        return cat_data.return_value

    def mock_get_last_results(self):
        cat_data = self.mock_object('PsDataProvider.get_last_status', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        cat_data.return_value = ''
        return cat_data.return_value


    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')


    def mock_assortment(self):
        # return self.gsk_generator.tool_box.mock_object('Assortment')
        return self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')
        # cat_data = self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')
        # cat_data.return_value = ''
        # return MagicMock()


    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider'), self.mock_object(
            'DbUsers')

    def mock_ps_data_provider_project_connector(self):
        self.mock_object('PSProjectConnector')

        # return self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')


    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')



    def mock_ps_data_provider_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')


    def mock_ps_data(self):
        return self.mock_object('PsDataProvider')


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
        self.assertEquals(self.gsk_generator.tool_box.calculate_sos(df, filters_num, filters_den, sos_policy),[9,2,3])