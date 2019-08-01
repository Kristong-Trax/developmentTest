from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.MARSUAE_SAND.Tests.data_test_unit_marsuae_sand import DataTestUnitMarsuae
from Projects.MARSUAE_SAND.Utils.KPIToolBox import MARSUAE_SANDToolBox
import pandas as pd



__author__ = 'natalyak'


class TestMarsuaeSand(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Projects.MARSUAE_SAND.Utils.KPIToolBox'

    def set_up(self):
        super(TestMarsuaeSand, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.db_users_mock = self.mock_db_users()
        self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        # self.probe_group_mock = self.mock_probe_group()
        self.full_store_info = self.mock_store_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.mock_all_products()
        self.mock_all_templates()
        self.mock_block()

    def mock_kpi_external_targets_data(self):
        external_targets_df = pd.read_excel(DataTestUnitMarsuae.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('MARSUAE_SANDToolBox.get_kpi_result_values_df')
        kpi_result_value.return_value = DataTestUnitMarsuae.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_store_data(self):
        store_data = self.mock_object('MARSUAE_SANDSceneToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitMarsuae.store_data_sss_a
        return store_data.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_db_users(self):
        return self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2'), self.mock_object('DbUsers')

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitMarsuae.kpi_static_data
        return static_kpi.return_value

    # def mock_all_products(self):
    #     self.data_provider_data_mock['all_products'] = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1,
    #                                                                  sheetname='all_products')

    # def mock_all_templates(self):
    #     self.data_provider_data_mock['all_templates'] = DataTestUnitPEPSICOUK.all_templates

    # def mock_scene_item_facts(self, data):
    #     self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    # def mock_block(self):
    #     self.mock_object('Block')
    #