
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
#     sessions = ['7d602b07-cccd-443d-bd71-ca5484bd41de', '31f63bc0-19d6-41df-9a24-c62a2f4f822d']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCIINBEVCICalculations(data_provider, output).run_project_calculations()
