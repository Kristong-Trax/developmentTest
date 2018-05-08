
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCRUFIFA.KPIGenerator import CCRUFIFAGenerator

__author__ = 'uri'


class CCRUFIFACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCRUFIFAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('ccrufifa calculations')
#     Config.init()
#     project_name = 'ccrufifa'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'BE109916-054E-4A9F-BBB7-C9B5579E7E4D'
#     data_provider.load_session_data(session)
#     output = Output()
#     CCRUFIFACalculations(data_provider, output).run_project_calculations()
