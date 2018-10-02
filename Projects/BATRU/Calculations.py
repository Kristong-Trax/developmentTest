
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU.KPIGenerator import BATRUGenerator
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'uri'


class BATRUCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        BATRUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru'
#     sessions = [
#          'efe1c553-79a8-4be9-a753-1981bc91fdc9',
#         'B342DF96-9852-4CDE-A86B-523795FD3294']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRUCalculations(data_provider, output).run_project_calculations()
