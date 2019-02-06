
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOTW.KPIGenerator import DIAGEOTWGenerator

__author__ = 'Nimrod'


class DIAGEOTWCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOTWGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageotw calculations')
#     Config.init()
#     project_name = 'diageotw'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'E9C9D024-5CD2-46F1-A759-2E527207B161'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOTWCalculations(data_provider, output).run_project_calculations()
