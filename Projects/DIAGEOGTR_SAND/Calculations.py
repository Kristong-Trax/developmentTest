
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGTR_SAND.KPIGenerator import DIAGEOGTRGenerator

__author__ = 'Yasmin'


class DIAGEOGTRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGTRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageogtr-sand calculations')
#     Config.init()
#     project_name = 'diageogtr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '38d2ac08-4f44-49be-b7c7-3fd8c86e8715'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGTRCalculations(data_provider, output).run_project_calculations()
