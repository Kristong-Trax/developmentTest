
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.SOLARBR.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



#
# if __name__ == '__main__':
#     LoggerInitializer.init('SolarBr calculations')
#     Config.init()
#     project_name = 'solarbr'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     list_sessions = sessions = ['33eac77e-a9ef-4b31-b734-8610cc4bbc15']
#
#
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         Calculations(data_provider, output).run_project_calculations()
