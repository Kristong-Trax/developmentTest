
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVCI_SAND.KPIGenerator import INBEVCISANDGenerator

__author__ = 'Elyashiv'


class INBEVCISANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVCISANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevci-sand calculations')
#     Config.init()
#     project_name = 'inbevci-sand'
#     sessions = ['fc314c82-bc68-4786-92a4-f55765593779']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCISANDCalculations(data_provider, output).run_project_calculations()
