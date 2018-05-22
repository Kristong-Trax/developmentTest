
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCBR_SAND.KPIGenerator import CCBRGenerator

__author__ = 'ilays'


class CCBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccbr-sand calculations')
#     Config.init()
#     project_name = 'ccbr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     list_sessions = ['fd89b261-fc0f-477d-86de-79f493d94dcf',
#                     '5d34ea92-fac2-4284-aef1-7783dcf86e80',
#                      'ff6e4cb2-89b6-4268-8cba-1844c6d55472',
#                     'b59b15ca-da10-43f6-bace-100012be8c30',
#                     '6c66c955-f3af-4688-9463-5441c8cc4c90',
#                     'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#                     'f92ebe9b-fa0d-49ec-b4a3-53928e7f886c',
#                     'dfedd3ef-9367-4bc3-b808-87460966c01d',
#                     'c02cbdf9-2555-4734-99e3-11623c1abddf']
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         CCBRCalculations(data_provider, output).run_project_calculations()
