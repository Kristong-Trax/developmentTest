
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCZA_SAND.KPIGenerator import CCZAGenerator

__author__ = 'Elyashiv'


class CCZACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCZAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccza-sand calculations')
#     Config.init()
#     project_name = 'ccza-sand'
#     sessions = ['1da43cbc-cdf2-4dd0-bbc9-121784e8a579']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         print session
#         data_provider.load_session_data(session)
#         output = Output()
#         CCZACalculations(data_provider, output).run_project_calculations()
