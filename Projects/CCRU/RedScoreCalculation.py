from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Trax.Utils.Logging.Logger import Log
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCRU.Utils.ToolBox import CCRUKPIToolBox

# from Projects.CCRU.Sets.Canteen import CCRUCanteenCalculations
# from Projects.CCRU.Sets.FT import CCRUFTCalculations
# from Projects.CCRU.Sets.FastFood import CCRUFastFoodCalculations
# from Projects.CCRU.Sets.HoReCa import CCRUHoReCaCalculations
# from Projects.CCRU.Sets.Hypermarket import CCRUHypermarketCalculations
# from Projects.CCRU.Sets.Petrol import CCRUPetrolCalculations
# from Projects.CCRU.Sets.Superette import CCRUSuperetteCalculations
# from Projects.CCRU.Sets.Supermarket import CCRUSupermarketCalculations

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

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()  # use log.time_message
        ps_data = PsDataProvider(self.data_provider, self.output)
        store_area = ps_data.store_area
        tool_box = CCRUKPIToolBox(self.data_provider, self.output, store_area)
        external_session_id = str(tool_box.external_session_id)
        if external_session_id.find('EasyMerch-P') < 0:

            kpi_set_name = tool_box.set_name
            test_store = ps_data.get_ps_store_info(self.data_provider['store_info'])['test_store']

            # if kpi_set_name == CANTEEN:
            #     CCRUCanteenCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == PETROL:
            #     CCRUPetrolCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == HORECA:
            #     CCRUHoReCaCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == FT:
            #     CCRUFTCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == HYPERMARKET:
            #     CCRUHypermarketCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == SUPERMARKET:
            #     CCRUSupermarketCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == SUPERETTE:
            #     CCRUSuperetteCalculations(self.data_provider, self.output, store_area).main_function()
            # elif kpi_set_name == FAST_FOOD:
            #     CCRUFastFoodCalculations(self.data_provider, self.output, store_area).main_function()

            if kpi_set_name == FT2018:
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
                Log.info('Session Store ID {} is a test store'.format(tool_box.store_id))
            else:
                Log.error('Session Store ID {} cannot be calculated. POS KPI Set name in store attribute is invalid: {}'
                          ''.format(tool_box.store_id, kpi_set_name))

        else:
            Log.info('Promo session, no Custom KPI calculation implied')

        # self.timer.stop('CCRUCalculations.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        '1D6B3586-6A76-4963-BF9F-F8B2E2537ADB',
        'FFF338AF-9505-4354-A2B7-835BF78090F3',
        'FF81168A-C990-40C2-943D-F68FC9AAACA2',
        'ffff2c80-697c-4e61-996e-893142806c55',
        'FFF99139-8B36-4272-92CE-E3A9696B819C',
        'ffd035af-1396-4b79-a7ac-25210dc8f995',
        'fe7080f5-3969-4005-a890-3afe5ffe6960',
        'FC8D623A-0DA1-4305-9DBF-9D2CC1301F38',
        'FE6D7CDF-161E-46BE-91A3-28B300ED35B0',
        'F66CE76B-4B52-4E4D-870D-0A1B0EFCEB17',
        'FA6E6AC6-C606-4195-B28C-932B07A289E5',
        'FEBEF96B-C8F9-47E4-BDD3-5087D4AB7121',
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()
