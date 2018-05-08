
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGCAREUS.KPIGenerator import PNGCAREUSPNGCAREUSGenerator

__author__ = 'Ortal'


class PNGCAREUSPNGCAREUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGCAREUSPNGCAREUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngcareus-sand calculations')
#     Config.init()
#     project_name = 'pngcareus'
#     data_provider = KEngineDataProvider(project_name)
#     session = '86bc23c1-6be6-4bc5-8d5d-36f0d951bd2a'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGCAREUSPNGCAREUSCalculations(data_provider, output).run_project_calculations()
