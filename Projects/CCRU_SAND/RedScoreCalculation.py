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
HRC_RESTAURANT_2018 = 'Pos 2018 - HoReCa (Restaurant/Cafe)'
HRC_COFFEE_2018 = 'Pos 2018 - HoReCa (Cofee /Tea Shops)'
HRC_BAR_2018 = 'Pos 2018 - HoReCa (Bar Tavern/Night Clubs)'
FAST_FOOD_2018 = 'Pos 2018 - QSR'
SUPERMARKET_2018 = 'Pos 2018 - MT - Supermarket'
QSR_2018 = 'Pos 2018 - QSR'


class CCRU_SANDCalculations(BaseCalculationsScript):
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
            Log.info('Session store "{}" is a test store'.format(
                tool_box.session_info.store_type))
        else:
            Log.error('Session store "{}" is not set to calculation'.format(
                tool_box.session_info.store_type))  # todo add all supported store types

        self.timer.stop('CCRU_SANDCalculations.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = ['9499fda2-84ae-45a4-a6cd-cc47067bb7f5',
                    '13f1bd6d-14c1-4b75-8f0c-bd5d6938c2c0',
                    '2a798e4a-6457-451f-9e6c-7362a262a4b3',
                    'dab5494b-bd19-4fbf-bfd4-7bf2e6832bdb',
                    'f8a4fe58-0333-42ef-934e-ad71a9e964d9']
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
