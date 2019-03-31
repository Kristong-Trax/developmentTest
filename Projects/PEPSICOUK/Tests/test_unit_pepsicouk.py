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
from Projects.PEPSICOUK.Tests.data_test_unit_ccbza_sand import DataTestUnitPEPSICOUK, DataScores, SCIFDataTestCCBZA_SAND, MatchProdSceneDataTestCCBZA_SAND
from Trax.Algo.Calculations.Core.DataProvider import Output
from mock import patch
import os
import pandas as pd
# from Projects.CCBZA_SAND.Utils.KPIToolBox import KPI_TAB, KPI_TYPE, PLANOGRAM_TAB, PRICE_TAB, SURVEY_TAB, AVAILABILITY_TAB, SOS_TAB, COUNT_TAB, \
#     SET_NAME, KPI_NAME, ATOMIC_KPI_NAME, SCORE, TARGET, SKU, POS, OTHER, MAX_SCORE

__author__ = 'natalya'


class Test_PEPSICOUK(MockingTestCase):
    # seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.PEPSICOUK.Utils.KPIToolBox'

    def set_up(self):
        # super(TestCCBZA_SAND, self).setUp()
        super(Test_PEPSICOUK, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.project_connector_mock = self.mock_project_connector()
        self.ps_dataprovider_project_connector_mock = self.mock_ps_data_provider_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.session_info_mock = self.mock_session_info()
        self.full_store_data_mock = self.mock_store_data()

        self.probe_groups_mock = self.mock_probe_groups()
        self.custom_entity_data_mock = self.mock_custom_entity_data()

        self.exclusion_template_mock = self.mock_template_data()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.external_targets_mock = self.mock_kpi_external_targets_data()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.kpi_scores_values_mock = self.mock_kpi_score_value_table()
        self.assortment_mock = self.mock_assortment_object()

    def mock_assortment_object(self):
        return self.mock_object('Assortment', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_external_targets_data(self):
        external_targets_df = pd.read_excel(DataTestUnitPEPSICOUK.external_targets)
        external_targets = self.mock_object('PEPSICOUKCommonToolBox.get_all_kpi_external_targets',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox',
                                            value=external_targets_df)
        # external_targets.return_value = external_targets_df
        return external_targets.return_value

    def mock_probe_groups(self):
        probe_groups_df = pd.read_excel(DataTestUnitPEPSICOUK.test_case_1, sheetname='stitch_groups')
        probe_groups = self.mock_object('PEPSICOUKToolBox.get_probe_group',
                                        value=probe_groups_df)
        # probe_groups.return_value = probe_groups_df
        return probe_groups.return_value

    def mock_custom_entity_data(self):
        custom_entities = self.mock_object('PEPSICOUKCommonToolBox.get_custom_entity_data',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox',
                                           value=DataTestUnitPEPSICOUK.custom_entity)
        # custom_entities.return_value = DataTestUnitPEPSICOUK.custom_entity
        return custom_entities.return_value

    def mock_common_project_connector(self):
        return self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2',
                                      value=DataTestUnitPEPSICOUK.kpi_static_data)
        # static_kpi.return_value = DataTestUnitPEPSICOUK.kpi_static_data
        return static_kpi.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

    def mock_project_connector(self):
        return self.mock_object('PSProjectConnector')

    def mock_ps_data_provider_project_connector(self):
        return self.mock_object('PsDataProvider.rds_connection', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')

    # def mock_static_kpi(self):
    #     static_kpi = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_static_data',
    #                                   value=DataTestUnitPEPSICOUK.kpi_static_data,
    #                                   path='Projects.PEPSICOUK.Utils.CommonToolBox')
    #     # static_kpi.return_value = DataTestUnitCCBZA_SAND.static_data
    #     return static_kpi.return_value

    def mock_store_data(self):
        store_data = self.mock_object('PEPSICOUKToolBox.get_store_data_by_store_id')
        store_data.return_value = DataTestUnitPEPSICOUK.store_data
        return store_data.return_value

    def mock_template_data(self):
        template_df = pd.read_excel(DataTestUnitPEPSICOUK.exclusion_template_path)
        template_data_mock = self.mock_object('PEPSICOUKCommonToolBox.get_exclusion_template_data',
                                              path='Projects.PEPSICOUK.Utils.CommonToolBox',
                                              value=template_df)
        # template_data_mock.return_value = template_df
        return template_data_mock.return_value

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
                                            path='Projects.PEPSICOUK.Utils.CommonToolBox')
        kpi_result_value.return_value = DataTestUnitPEPSICOUK.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_kpi_score_value_table(self):
        kpi_score_value = self.mock_object('PEPSICOUKCommonToolBox.get_kpi_result_values_df',
                                           path='Projects.PEPSICOUK.Utils.CommonToolBox',)
        kpi_score_value.return_value = DataTestUnitPEPSICOUK.kpi_scores_values_table
        return kpi_score_value.return_value

