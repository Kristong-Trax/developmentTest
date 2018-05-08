
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCBR_SAND.KPIGenerator import CCBRGenerator

__author__ = 'ilays'


class CCBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccbr-sand calculations', 'ccbr-sand calculations')
#     Config.init()
#     project_name = 'ccbr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     list_sessions = ['07da1397-86ec-4b13-a74d-64a26b053953',
#                     '58685ad0-dc03-4c18-ac85-abb2f440e273',
#                     '4b3a0dab-c625-4271-ae6b-4e7e5a1c159b',
#                     'dfedd3ef-9367-4bc3-b808-87460966c01d',
#                     'c02cbdf9-2555-4734-99e3-11623c1abddf']
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         CCBRCalculations(data_provider, output).run_project_calculations()
