from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Utils.Logging.Logger import Log


from Projects.CCRU_SAND.Sets.Canteen import CCRU_SANDCanteenCalculations
from Projects.CCRU_SAND.Sets.FT import CCRU_SANDFTCalculations
from Projects.CCRU_SAND.Sets.FastFood import CCRU_SANDFastFoodCalculations
from Projects.CCRU_SAND.Sets.HoReCa import CCRU_SANDHoReCaCalculations
from Projects.CCRU_SAND.Sets.Hypermarket import CCRU_SANDHypermarketCalculations
from Projects.CCRU_SAND.Sets.Petrol import CCRU_SANDPetrolCalculations
from Projects.CCRU_SAND.Sets.Superette import CCRU_SANDSuperetteCalculations
from Projects.CCRU_SAND.Sets.Supermarket import CCRU_SANDSupermarketCalculations

from Projects.CCRU_SAND.Sets.FT2018 import CCRU_SANDFT2018Calculations
from Projects.CCRU_SAND.Sets.Hypermarket2018 import CCRU_SANDHypermarket2018Calculations
from Projects.CCRU_SAND.Sets.HoReCaRestaurant2018 import CCRU_SANDHRCRestaurant2018Calculations
from Projects.CCRU_SAND.Sets.HoReCaCoffee2018 import CCRU_SANDHRCCoffee2018Calculations
from Projects.CCRU_SAND.Sets.HoReCaBar2018 import CCRU_SANDHRCBar2018Calculations
from Projects.CCRU_SAND.Sets.Canteen2018 import CCRU_SANDCanteen2018Calculations
from Projects.CCRU_SAND.Sets.Supermarket2018 import CCRU_SANDSupermarket2018Calculations
from Projects.CCRU_SAND.Sets.ConvenienceBig import CCRU_SANDConvenienceBigCalculations
from Projects.CCRU_SAND.Sets.ConvenienceSmall import CCRU_SANDConvenienceSmallCalculations
from Projects.CCRU_SAND.Sets.QSR2018 import CCRU_SANDQsr2018Calculations
from Projects.CCRU_SAND.Sets.Petrol2018 import CCRU_SANDPetrol2018Calculations

from Projects.CCRU_SAND.Utils.ToolBox import CCRU_SANDKPIToolBox
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime


__author__ = 'urid'

HYPERMARKET_MARKETS = ['Hypermarket', 'Cash & Carry']
SUPERMARKET_MARKETS = ['Supermarket']
SUPERETTE_EXCLUSIONS = ['Hypermarket', 'Cash & Carry', 'Supermarket']

CANTEEN = 'PoS 2017 - Canteen'
PETROL = 'PoS 2017 - Petroleum'
HORECA = 'PoS 2017 - HoReCa'
FT = 'PoS 2017 - FT'
HYPERMARKET = 'PoS 2017 - MT - Hypermarket'
SUPERMARKET = 'PoS 2017 - MT - Supermarket'
SUPERETTE = 'PoS 2017 - MT - Superette'
FAST_FOOD = 'PoS 2017 - QSR'

FT2018 = 'Pos 2018 - FT'
CONVENIENCE_BIG_2018 = 'Pos 2018 - MT - Convenience Big'
CONVENIENCE_SMALL_2018 = 'Pos 2018 - MT - Convenience Small'
CANTEEN_2018 = 'Pos 2018 - Canteen'
PETROL_2018 = 'Pos 2018 - Petroleum'
CBS = 'Pos 2018 - MT - Convenience Big'
CSS = 'Pos 2018 - MT - Convenience Small'
HYPERMARKET_2018 = 'Pos 2018 - MT - Hypermarket'
HRC_RESTAURANT_2018 = 'Pos 2018 - HoReCa - Restaurant Cafe'
HRC_COFFEE_2018 = 'Pos 2018 - HoReCa - Coffee Tea Shops'
HRC_BAR_2018 = 'Pos 2018 - HoReCa - Bar Tavern Night Clubs'
FAST_FOOD_2018 = 'Pos 2018 - QSR'
SUPERMARKET_2018 = 'Pos 2018 - MT - Supermarket'
QSR_2018 = 'Pos 2018 - QSR'


class CCRU_SANDCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        ps_data = PsDataProvider(self.data_provider, self.output)
        store_area = ps_data.store_area
        tool_box = CCRU_SANDKPIToolBox(self.data_provider, self.output, store_area)
        kpi_set_name = tool_box.set_name
        test_store = ps_data.get_ps_store_info(self.data_provider['store_info'])['test_store']
        if kpi_set_name == CANTEEN:
            CCRU_SANDCanteenCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == PETROL:
            CCRU_SANDPetrolCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HORECA:
            CCRU_SANDHoReCaCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == FT:
            CCRU_SANDFTCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HYPERMARKET:
            CCRU_SANDHypermarketCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERMARKET:
            CCRU_SANDSupermarketCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERETTE:
            CCRU_SANDSuperetteCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == FAST_FOOD:
            CCRU_SANDFastFoodCalculations(self.data_provider, self.output, store_area).main_function()

        elif kpi_set_name == FT2018:
            CCRU_SANDFT2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CANTEEN_2018:
            CCRU_SANDCanteen2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == PETROL_2018:
            CCRU_SANDPetrol2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HYPERMARKET_2018:
            CCRU_SANDHypermarket2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERMARKET_2018:
            CCRU_SANDSupermarket2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_RESTAURANT_2018:
            CCRU_SANDHRCRestaurant2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_COFFEE_2018:
            CCRU_SANDHRCCoffee2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_BAR_2018:
            CCRU_SANDHRCBar2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CONVENIENCE_BIG_2018:
            CCRU_SANDConvenienceBigCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CONVENIENCE_SMALL_2018:
            CCRU_SANDConvenienceSmallCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == QSR_2018:
            CCRU_SANDQsr2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif test_store.values[0] == "Y":
            Log.info('Session Store ID {} is a test store'.format(tool_box.store_id))
        else:
            Log.error('Session Store ID {} cannot be calculated. POS KPI Set name in store attribute is invalid: {}'.format(tool_box.store_id, kpi_set_name))

        # self.timer.stop('CCRU_SANDCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('CCRU calculations')
#     Config.init()
#     project_name = 'ccru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session_uids = [
#                     # '80ACE99D-74B8-4A8B-B054-CC0BBACD69C5',
#                     # '8849CA37-1472-45D3-B10E-B7B08B2FD07A',
#                     # '392D0C3C-BDE7-46C2-B0AE-FF460C0A219F',
#                     # '6F121346-EDE4-48F4-B3E8-779DB6F32B6E',
#                     # '13585b79-2310-4882-8b93-8aeaaea1300d',
#                     # 'c5c7869c-c20d-4d53-ba6f-9496041811c9',
#                     # '2afbc9aa-b187-49f6-a550-dff24748484e',
#                     # 'edc0638e-8c7e-4d8f-b466-568233b7f6ce',
#                     # '2da78f81-e600-4753-a46c-2db332daba38',
#                     'bc1d30b8-7f7a-4be4-91b9-911f20d33c37',
#                     # 'FC8ACBD2-34A7-4C47-AE50-A1CA5EC30C28',
#                     # '285A9D0D-6753-4C17-9AD9-DB61493EA0A3',
#                     # '58F85A60-0C5B-4265-9F85-C93C375677DC',
#                     # '9616F82D-DB14-4321-B542-FE3CB832F599',
#                     # '223128E8-955D-42BD-B802-D5883F99D0A7',
#                     # '0781CFDE-8232-4F8F-A120-4CB3575278E2',
#                     # '099CF9CB-BA5F-400A-92BE-3FC1D682E712',
#                     # '197A5218-1D3E-4F85-BF09-4F028911DF6B',
#                     # '441ECF0A-2D4E-484E-8EB9-A547B79DD62B',
#                     # 'E20289C8-AF64-433D-9B0C-4069867C2C6D',
#                     # '4268F9E2-EE1F-408A-92E1-C86DEF0DC2EF',
#                     'A2B0EE9D-960A-4180-AACD-D60DE46A0AB7',
#                     # '5db3f7b7-beb1-4895-8f2e-cc40e7b1a8fa',
#                     # 'ecc12805-dcb4-442a-9335-007d89d09d18',
#                     # '5e105808-a74c-4280-886d-c7054bc12fa0',
#                     # '35115FE8-314A-41F0-B280-0D3288F11CF3',
#                     # 'CD0DD19D-D87E-46B7-A203-1CBFB05EDE29',
#                     # 'D31058A3-455F-43ED-8499-CFF58960BD42',
#                     # 'FB712B01-4647-4672-BB3C-B9675848D72A',
#                     # '0DB9017A-2951-4BD9-9ABC-D949E079EAA8',
#                     # '3dc2f71f-d1e9-4b0a-8b14-d3b73ce90096',
#                     # 'e046340a-7a57-4e4c-9a57-2611a2191fa1',
#                     # '6c155181-e973-4321-b1ec-6106eb66df12',
# ]
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCRU_SANDCalculations(data_provider, output).run_project_calculations()
