
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
#     sessions = ['75a1bacc-d927-46fa-9842-d91bf56dd69f',
#                 'cf1f9d15-a4a1-4863-8fac-c9b4ed8f45eb',
#                 '76607522-3199-4844-aab6-8114e78edf25',
#                 'eb39d72c-b9cc-441d-9e7b-031d1babb313',
#                 '78a6aaba-c5d6-4dd7-9fbb-8423792af0d1',
#                 ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRUCalculations(data_provider, output).run_project_calculations()
