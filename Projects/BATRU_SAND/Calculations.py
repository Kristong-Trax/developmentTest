
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU_SAND.KPIGenerator import BATRU_SANDGenerator

__author__ = 'uri'


class BATRU_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        BATRU_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '3c68a151-9909-4f6d-8e98-a2e607eb8a86'
#     data_provider.load_session_data(session)
#     output = Output()
#     BATRU_SANDCalculations(data_provider, output).run_project_calculations()
#     sessions = [
#         '76607522-3199-4844-aab6-8114e78edf25',
#         'eb39d72c-b9cc-441d-9e7b-031d1babb313',
#         '78a6aaba-c5d6-4dd7-9fbb-8423792af0d1',
#     ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRU_SANDCalculations(data_provider, output).run_project_calculations()
