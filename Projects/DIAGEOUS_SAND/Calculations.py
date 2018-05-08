
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOUS_SAND.KPIGenerator import DIAGEOUSGenerator

__author__ = 'uri'


class DIAGEOUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageous-sand calculations')
#     Config.init()
#     project_name = 'diageous-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '64A18BEA-58B9-41C5-82FC-14082411C214'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOUSCalculations(data_provider, output).run_project_calculations()
