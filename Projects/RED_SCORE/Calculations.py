
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.RED_SCORE.KPIGenerator import REDGenerator

__author__ = 'Ilan'


class REDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        REDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('red-score calculations')
#     Config.init()
#     project_name = 'red-score'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     REDCalculations(data_provider, output).run_project_calculations()
