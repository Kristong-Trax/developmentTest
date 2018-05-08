
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.RNBDE.KPIGenerator import RNBDEGenerator

__author__ = 'uri'


class RNBDERNBDECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RNBDEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('rnbde calculations')
#     Config.init()
#     project_name = 'rnbde'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'f928af7c-6c2b-427e-b290-d709eb93008c'
#     sessions = [
#         'b9391ac1-e836-4ff9-8883-0ccf0f571989'
#
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         RNBDERNBDECalculations(data_provider, output).run_project_calculations()
