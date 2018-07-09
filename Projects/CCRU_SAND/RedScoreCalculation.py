from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

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


# if __name__ == '__main__':
#     LoggerInitializer.init('CCRU calculations')
#     Config.init()
#     project_name = 'ccru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session_uids = ['b91e9d71-59aa-4f0f-9ed6-52a9fa3c717a',
#                     'BA767F58-C9CE-4856-AE02-847BC2C91615',
#                     '4fe08643-4b59-46be-9721-bf4623da5136',
#                     '54E4F8D4-EA57-4AAF-9C68-28C72D725C84',
#                     'ae6167c7-cc11-475f-85bf-6b61e7eea2e7',
#                     'F00622E0-3804-4FE5-AAF1-16CDADA29254',
#                     'EA675273-C613-412D-9730-36CDA35CB034',
#                     'B5301577-9E6B-46A9-A8C3-1A6BF41C960F',
#                     '1879669b-2e7a-43e6-9822-9dcc5b00ba81',
#                     '7085C2F1-A4EF-48C5-B6EF-97E3BBA0F776',
#                     'bb6ba43d-75e3-4e12-8054-29ec40f9da9c',
#                     '233EED07-1E74-4970-AB0F-CC52CD9A127C',
#                     '6d4bfa21-23ea-4a4e-82ce-bbff3c22fd8f',
#                     '6EB7EFC6-943E-4864-8ADB-3DB5D65F6DC7',
#                     '7d3a0cf4-1968-4c5b-a964-2e794114c19c',
#                     '1f4058d5-a7ad-40be-bebd-f90bdf366ad6',
#                     'EBF5206C-B0E9-48B8-9155-C4FFB380E0E1',
#                     'B2F56C86-D1AC-4DB7-B6CD-8A41DDE78F26',
#                     '9E9F3FE8-71AF-474E-809C-9990A77A02D4',
#                     '7B843A7E-3782-449D-A47A-3CAA626B4EB4',
#                     'E0485117-9044-408B-92DD-8B08056AA32F',
#                     'AFB3A1A2-EFC2-4155-823D-399009E11197',
#                     '877BF06D-6AFA-4126-8ABF-8C71847FB8AA',
#                     'F30CC7B0-DCCF-486B-BB9B-90C5F4A4EA9E',
#                     'FE897A16-420E-4376-B604-5A6D1E489F8A',
#                     '736a9b81-2bb2-46fd-b0b7-2b1448742184',
#                     '1237cb14-b81f-4a97-9660-a02bb4d8db86',
#                     'c384fe45-9f83-4206-b06a-837974bf5ee2',
#                     'EE09990A-B36C-47FF-9317-0C1E640F79C2',
#                     'ac227123-35c2-4aee-93e8-4f4ede858f79',
#                     'C99F0438-C381-4652-BD84-3BBB3E851396',
#                     '8B633DE4-BB8D-4DFA-AB68-49B4EC59583F',
#                     '3b52184e-47ab-4b3a-bb52-15699a256d9a',
#                     '5F2EFCF2-8C1F-4879-827D-4B87670DBA17',
#                     '3ef5be60-0b06-4d63-9d62-97c4ba91561a',
#                     'A6CCC04D-DB3E-477F-A77F-5729ADD7958A',
#                     '3991c4e9-a768-45e8-8828-9354f764d3c7',
#                     'c6c4982e-a46d-4374-91b0-16ec1e17cae5',
#                     'DE603845-D86E-41A6-8359-20E11E0AA9D4',
#                     '2885176A-1137-4F92-8257-3E00CBBCAD49',
#                     '57d81ca1-ea86-41c4-b2ff-65683e519e0a',
#                     '1e8a6a54-14c0-405f-ab4a-552641f4baf8']
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCRU_SANDCalculations(data_provider, output).run_project_calculations()
