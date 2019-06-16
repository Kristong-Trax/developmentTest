
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

#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevci calculations')
#     Config.init()
#     project_name = 'inbevci'
#     sessions = ['fe662593-60fe-4edf-b06b-015de50d78d6']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCIINBEVCICalculations(data_provider, output).run_project_calculations()
