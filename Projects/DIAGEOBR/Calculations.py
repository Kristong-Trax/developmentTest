
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBR.KPIGenerator import DIAGEOBRGenerator

__author__ = 'Nimrod'


class DIAGEOBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageobr'
#     data_provider = KEngineDataProvider(project_name)
#     session = '27eecc1d-bfb7-4493-9ba2-93883267e8e3'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOBRCalculations(data_provider, output).run_project_calculations()
