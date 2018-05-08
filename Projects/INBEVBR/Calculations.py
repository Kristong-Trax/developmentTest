
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVBR.KPIGenerator import INBEVBRGenerator

__author__ = 'Yasmin'


class INBEVBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbr calculations')
#     Config.init()
#     project_name = 'InbevBr'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'a1521669-d851-4a17-ada9-9e8d95370c3f'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVBRCalculations(data_provider, output).run_project_calculations()
