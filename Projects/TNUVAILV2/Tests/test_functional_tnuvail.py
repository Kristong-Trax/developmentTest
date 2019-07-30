# coding=utf-8
from Projects.TNUVAILV2.Tests.Data.TestData.test_data_tnuvailv2 import TnuvailV2SanityData
# from Projects.TNUVAILV2.Tests.Data.MockDataFrames import TnuvaMocks
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
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
        # self.data_provider_mock = MagicMock()
        # self.output = MagicMock()
        # self.mock_common()
        # self.mock_project_connector()
        # self.tool_box = TNUVAILToolBox(self.data_provider_mock,  self.output)

    # def mock_util_functions(self):
    #     self.lvl3_assortment = self.mock_object('_prepare_data_for_assortment_calculation')
    #     self.lvl3_assortment.return_value = TnuvaMocks.lvl3_results()
    #
    # def mock_project_connector(self):
    #     # self.mock_object('PSProjectConnector')
    #     self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
    #     self.mock_object('PSProjectConnector', path=TestTnuvaV2.base_calculation_path)
    #
    # def mock_common(self):
    #     self.mock_object('Common')
    #     self.mock_object('Common', path=TestTnuvaV2.base_calculation_path)

    # @seeder.seed(["mongodb_products_and_brands_seed", "tnuvailv2_sand_seed"], TnuvailV2SanityData())
    # def _initiate_date_provider(self):
    #     data_provider = KEngineDataProvider(TnuvailV2SanityData.project_name)
    #     session = '236c1577-0ecb-4bf9-88b9-c9e87ab17c58'
    #     data_provider.load_session_data(session)
    #     return data_provider

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
        project_name = TnuvailV2SanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['236c1577-0ecb-4bf9-88b9-c9e87ab17c58']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            Calculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
