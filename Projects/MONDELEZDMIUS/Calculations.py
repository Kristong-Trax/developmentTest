
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.MONDELEZDMIUS.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#     from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
#     from Trax.Utils.Conf.Configuration import Config
#     LoggerInitializer.init('mondelezdmius calculations')
#     Config.init()
#     project_name = 'mondelezdmius'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['4cdc13af-d767-46d0-bf52-abe4045ccec9']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
