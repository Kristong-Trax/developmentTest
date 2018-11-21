
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGR.KPIGenerator import DIAGEOGRGenerator

__author__ = 'Nimrod'


class DIAGEOGRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageogr calculations')
#     Config.init()
#     project_name = 'diageogr'
#     data_provider = KEngineDataProvider(project_name)
#     session = '6C40C5E4-42D8-4606-947B-D31E072AC625'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGRCalculations(data_provider, output).run_project_calculations()
