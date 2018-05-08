
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.SOLARBR_SAND.KPIGenerator import SOLARBRGenerator

__author__ = 'Ilan'


class SOLARBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        SOLARBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('solarbr-sand calculations')
#     Config.init()
#     project_name = 'solarbr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '64af1634-9bfc-406d-9ebf-8aab9e6750cf'
#     data_provider.load_session_data(session)
#     output = Output()
#     SOLARBRCalculations(data_provider, output).run_project_calculations()
