
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'nidhin'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('DIAGEOIN calculations')
#     Config.init()
#     project_name = 'diageoin'
#     data_provider = KEngineDataProvider(project_name)
#     session = '92726422-1c2e-44a2-ab3d-6ed378bb7dac'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOINCalculations(data_provider, output).run_project_calculations()
