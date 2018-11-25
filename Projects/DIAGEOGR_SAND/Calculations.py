
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGR_SAND.KPIGenerator import DIAGEOGRSANDGenerator

__author__ = 'Nimrod'


class DIAGEOGRSANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGRSANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageogr calculations')
#     Config.init()
#     project_name = 'diageogr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '03573352-467F-4C73-BBFE-60FCB8A04B2C'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGRSANDCalculations(data_provider, output).run_project_calculations()
