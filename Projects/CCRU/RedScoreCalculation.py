from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Utils.Logging.Logger import Log


from Projects.CCRU.Sets.Canteen import CCRUCanteenCalculations
from Projects.CCRU.Sets.FT import CCRUFTCalculations
from Projects.CCRU.Sets.FastFood import CCRUFastFoodCalculations
from Projects.CCRU.Sets.HoReCa import CCRUHoReCaCalculations
from Projects.CCRU.Sets.Hypermarket import CCRUHypermarketCalculations
from Projects.CCRU.Sets.Petrol import CCRUPetrolCalculations
from Projects.CCRU.Sets.Superette import CCRUSuperetteCalculations
from Projects.CCRU.Sets.Supermarket import CCRUSupermarketCalculations

from Projects.CCRU.Sets.FT2018 import CCRUFT2018Calculations
from Projects.CCRU.Sets.Hypermarket2018 import CCRUHypermarket2018Calculations
from Projects.CCRU.Sets.HoReCaRestaurant2018 import CCRUHRCRestaurant2018Calculations
from Projects.CCRU.Sets.HoReCaCoffee2018 import CCRUHRCCoffee2018Calculations
from Projects.CCRU.Sets.HoReCaBar2018 import CCRUHRCBar2018Calculations
from Projects.CCRU.Sets.Canteen2018 import CCRUCanteen2018Calculations
from Projects.CCRU.Sets.Supermarket2018 import CCRUSupermarket2018Calculations
from Projects.CCRU.Sets.ConvenienceBig import CCRUConvenienceBigCalculations
from Projects.CCRU.Sets.ConvenienceSmall import CCRUConvenienceSmallCalculations
from Projects.CCRU.Sets.QSR2018 import CCRUQsr2018Calculations
from Projects.CCRU.Sets.Petrol2018 import CCRUPetrol2018Calculations

from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider


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


class CCRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        ps_data = PsDataProvider(self.data_provider, self.output)
        store_area = ps_data.store_area
        tool_box = CCRUKPIToolBox(self.data_provider, self.output, store_area)
        kpi_set_name = tool_box.set_name
        test_store = ps_data.get_ps_store_info(self.data_provider['store_info'])['test_store']
        if kpi_set_name == CANTEEN:
            CCRUCanteenCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == PETROL:
            CCRUPetrolCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HORECA:
            CCRUHoReCaCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == FT:
            CCRUFTCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HYPERMARKET:
            CCRUHypermarketCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERMARKET:
            CCRUSupermarketCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERETTE:
            CCRUSuperetteCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == FAST_FOOD:
            CCRUFastFoodCalculations(self.data_provider, self.output, store_area).main_function()

        elif kpi_set_name == FT2018:
            CCRUFT2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CANTEEN_2018:
            CCRUCanteen2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == PETROL_2018:
            CCRUPetrol2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HYPERMARKET_2018:
            CCRUHypermarket2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == SUPERMARKET_2018:
            CCRUSupermarket2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_RESTAURANT_2018:
            CCRUHRCRestaurant2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_COFFEE_2018:
            CCRUHRCCoffee2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == HRC_BAR_2018:
            CCRUHRCBar2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CONVENIENCE_BIG_2018:
            CCRUConvenienceBigCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == CONVENIENCE_SMALL_2018:
            CCRUConvenienceSmallCalculations(self.data_provider, self.output, store_area).main_function()
        elif kpi_set_name == QSR_2018:
            CCRUQsr2018Calculations(self.data_provider, self.output, store_area).main_function()
        elif test_store.values[0] == "Y":
            Log.info('Session store "{}" is a test store'.format(
                tool_box.session_info.store_type))
        else:
            Log.error('Session store "{}" is not set to calculation'.format(
                tool_box.session_info.store_type))  # todo add all supported store types

        self.timer.stop('CCRUCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('CCRU calculations')
#     Config.init()
#     project_name = 'ccru'
#     data_provider = KEngineDataProvider(project_name)
#     session_uids = ['851E8DC4-CC29-4F8E-AFB4-BE3E4C9921B9']
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCRUCalculations(data_provider, output).run_project_calculations()
#
# sessions in 2018 per store type:
# 'e3d4817b-3654-40a5-9d0a-3fed8f19bcba' --> Pos 2018 - MT - Convenience Big
# 'a9175991-91b5-4bd6-9b12-78ede2a6a578' --> 'Pos 2018 - MT - Convenience Small'
# '366e19c9-338d-49ad-b902-f680520f5862' --> Pos 2018 - FT
# 'C0A94463-627A-48AB-8B7C-B1FE7712F35B' --> Pos 2018 - MT - Hypermarket
# '38503654-2c64-436f-9064-aeac06d4c966' --> Pos 2018 - MT - Supermarket
# '5dbdb69e-257b-4dfe-9b42-c07675703167' -->Pos 2018 - Petroleum
# '95423BDE-F038-488B-A8D4-C7267528785D' --> Pos 2018 - QSR
