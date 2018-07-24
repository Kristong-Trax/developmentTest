
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU.KPIGenerator import BATRUGenerator

__author__ = 'uri'


class BATRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        BATRUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru'
#     # data_provider = KEngineDataProvider(project_name)
#     # session = '78a6aaba-c5d6-4dd7-9fbb-8423792af0d1'
#     # data_provider.load_session_data(session)
#     # output = Output()
#     # BATRUCalculations(data_provider, output).run_project_calculations()
#     sessions = ['cb9c8b64-6839-4094-a1bb-5416c31cf4e6',
#                 'a82e69c5-e38d-46b3-8043-b5a24c5a96a8',  # incorrect shelves
#                 'fd85b121-8d00-4309-bcb8-07b7b26f873b',
#                 'f1573fa7-1c5f-4935-bd5b-05ff798185f5']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRUCalculations(data_provider, output).run_project_calculations()
