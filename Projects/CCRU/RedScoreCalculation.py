from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

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
                Log.info('Session Store ID {} is a test store'.format(tool_box.store_id))
            else:
                Log.error('Session Store ID {} cannot be calculated. POS KPI Set name in store attribute is invalid: {}'.format(tool_box.store_id, kpi_set_name))

        else:
            Log.info('Promo session, no calculation implied')

        # self.timer.stop('CCRUCalculations.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('CCRU calculations')
#     Config.init()
#     project_name = 'ccru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session_uids = [
#         '42C08E8C-A434-4FDE-841B-66C121E8C568',
#         '246c80c3-63b9-4a90-b519-ca54416f1079',
#         '4a9e883e-811e-47e4-bb86-235e55df1875',
#         '48a19a3d-92b4-4f83-9b77-47ce004ff546',
#         'ad063eec-6f7c-4a6d-b9ea-aa2610d633e8',
#         '3480fa35-e83a-4b3a-8c6e-b251f6f0851a',
#         '2292DF71-FA8E-4AB9-A0C9-125CA9C9423F',
#         '9661AF7C-9871-4C5B-8533-406C2BD319B8',
#         'e3b7576d-a193-44c6-9b25-1a402ee14539',
#         '5bd39786-002b-4645-8867-4a3389b1206e',
#         '06799C75-F589-48A9-B68A-CB07958E5FD7',
#     ]
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCRUCalculations(data_provider, output).run_project_calculations()
