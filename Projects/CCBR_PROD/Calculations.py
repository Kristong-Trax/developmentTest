
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.CCBR_PROD.KPIGenerator import CCBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class CCBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('ccbr-prod calculations')
#     Config.init()
#     project_name = 'ccbr-prod'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     list_sessions = ['f61ba813-0990-4e6a-a614-3a78a9628626',
#                     '1b4d4795-90b9-4161-b321-cfba53ba5fc5',
#                     '233f5fca-8ce7-4398-8e8e-bb248de06b1a',
#                     '6c66c955-f3af-4688-9463-5441c8cc4c90',
#                     'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#                     'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#                     'dfedd3ef-9367-4bc3-b808-87460966c01d',
#                     'c02cbdf9-2555-4734-99e3-11623c1abddf']
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         CCBRCalculations(data_provider, output).run_project_calculations()
