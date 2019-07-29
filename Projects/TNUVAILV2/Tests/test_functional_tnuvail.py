# coding=utf-8
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Projects.TNUVAILV2.Utils.KPIToolBox import TNUVAILToolBox
from Trax.Data.Testing.SeedNew import Seeder
from mock import MagicMock

__author__ = 'idanr'


class TestTnuvaV2(TestFunctionalCase):
    """
    """
    # TODO !! This is just the beginning!

    base_calculation_path = 'KPIUtils_v2.Calculations.BaseCalculations'
    seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.TNUVAILV2.Utils.KPIToolBox'

    def set_up(self):
        super(TestTnuvaV2, self).set_up()
        self.data_provider_mock = MagicMock()
        self.output = MagicMock()
        self.mock_common()
        self.mock_project_connector()
        self.tool_box = TNUVAILToolBox(self.data_provider_mock,  self.output)

    def mock_project_connector(self):
        # self.mock_object('PSProjectConnector')
        self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path=TestTnuvaV2.base_calculation_path)

    def mock_common(self):
        self.mock_object('Common')
        self.mock_object('Common', path=TestTnuvaV2.base_calculation_path)

    # @seeder.seed(["mongodb_products_and_brands_seed", "????"], SanofiSanityData())
    # def _initiate_date_provider(self):
    #     data_provider = KEngineDataProvider(SanofiSanityData.project_name)
    #     session = ''
    #     data_provider.load_session_data(session)
    #     return data_provider
