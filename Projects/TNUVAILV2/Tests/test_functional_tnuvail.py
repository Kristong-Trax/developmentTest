# coding=utf-8
from Projects.TNUVAILV2.Tests.Data.TestData.test_data_tnuvailv2 import TnuvailV2SanityData
# from Projects.TNUVAILV2.Tests.Data.MockDataFrames import TnuvaMocks
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

# from Projects.TNUVAILV2.Utils.Consts import Consts
# from Projects.TNUVAILV2.Utils.KPIToolBox import TNUVAILToolBox
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Projects.TNUVAILV2.Calculations import Calculations
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.SeedNew import Seeder
# from mock import MagicMock
import MySQLdb


__author__ = 'idanr'


class TestTnuvaV2(TestFunctionalCase):
    base_calculation_path = 'KPIUtils_v2.Calculations.BaseCalculations'
    seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.TNUVAILV2.Utils.KPIToolBox'

    def set_up(self):
        super(TestTnuvaV2, self).set_up()
        self.data_provider_mock = self._initiate_date_provider()
    #     self.output = MagicMock()
    #     self.mock_common()
    #     self.mock_project_connector()
    #     self.tool_box = TNUVAILToolBox(self.data_provider_mock,  self.output)
    #
    # def mock_util_functions(self):
    #     self.lvl3_assortment = self.mock_object('_prepare_data_for_assortment_calculation')
    #     self.lvl3_assortment.return_value = TnuvaMocks.lvl3_results()
    #
    # def mock_project_connector(self):
    #     # self.mock_object('PSProjectConnector')
    #     self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
    #     self.mock_object('PSProjectConnector', path='Projects.TNUVAILV2.Utils.DataBaseHandler')
    #     self.mock_object('PSProjectConnector', path=TestTnuvaV2.base_calculation_path)
    #
    # def mock_common(self):
    #     self.mock_object('Common')
    #     self.mock_object('Common', path=TestTnuvaV2.base_calculation_path)
    #
    # def mock_ps_data_provider(self):
    #     self.mock_object('PsDataProvider')
    #     self.mock_object('PsDataProvider', path=TestTnuvaV2.base_calculation_path)

    @seeder.seed(["mongodb_products_and_brands_seed", "tnuvailv2_sand_seed"], TnuvailV2SanityData())
    def _initiate_date_provider(self):
        data_provider = KEngineDataProvider(TnuvailV2SanityData.project_name)
        session = '236c1577-0ecb-4bf9-88b9-c9e87ab17c58'
        data_provider.load_session_data(session)
        return data_provider

    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @seeder.seed(["mongodb_products_and_brands_seed", "tnuvailv2_sand_seed"], TnuvailV2SanityData())
    def test_tnuvailv2_sanity(self):
        output = Output()
        Calculations(self.data_provider_mock, output).run_project_calculations()
        self._assert_kpi_results_filled()

    # def test_general_sos_calculation(self):
    #     test_case_1 = ((11, 11), {Consts.MANUFACTURER_FK: self.tool_box.own_manufacturer_fk})
    #     test_case_2 = ((0, 11), {Consts.MANUFACTURER_FK: 999})
    #     test_case_3 = ((1, 11), {Consts.CATEGORY_FK: 252})
    #     test_cases = [test_case_1, test_case_2, test_case_3]
    #     for expected_res, test_case in test_cases:
    #         self.assertEqual(expected_res, self.tool_box._general_sos_calculation(self.tool_box.scif, **test_case))
