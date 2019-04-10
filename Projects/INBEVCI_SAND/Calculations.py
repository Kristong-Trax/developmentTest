
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


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevci-sand calculations')
#     Config.init()
#     project_name = 'inbevci-sand'
#     sessions = ['d2ce8588-cb7c-4b52-87a3-4ab2dfb632a7']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVCISANDCalculations(data_provider, output).run_project_calculations()
