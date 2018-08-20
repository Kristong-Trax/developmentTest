
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


if __name__ == '__main__':
    LoggerInitializer.init('batru calculations')
    Config.init()
    project_name = 'batru-sand'
    # data_provider = KEngineDataProvider(project_name)
    # session = '78a6aaba-c5d6-4dd7-9fbb-8423792af0d1'
    # data_provider.load_session_data(session)
    # output = Output()
    # BATRUCalculations(data_provider, output).run_project_calculations()
    sessions = [
                'ff6a0c1c-a846-4c9f-9eeb-33e798c9ae69',
                ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
