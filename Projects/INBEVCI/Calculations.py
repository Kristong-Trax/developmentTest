
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVCI.KPIGenerator import INBEVCIINBEVCIGenerator

__author__ = 'Elyashiv'


class INBEVCIINBEVCICalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVCIINBEVCIGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevci calculations')
#     Config.init()
#     project_name = 'inbevci'
#     sessions = ['52031109-0a29-4ae1-9f79-7a9b709b0f37']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCIINBEVCICalculations(data_provider, output).run_project_calculations()
