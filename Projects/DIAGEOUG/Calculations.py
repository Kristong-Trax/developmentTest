from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOUG.KPIGenerator import DIAGEOUGGenerator

__author__ = 'Jasmine'


class DIAGEOUGCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOUGGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoug calculations')
#     Config.init()
#     project_name = 'diageoug'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'f9d6b8a5-7964-4ef5-afe4-8580df97f57c'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOUGCalculations(data_provider, output).run_project_calculations()
