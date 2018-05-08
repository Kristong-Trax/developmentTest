
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU_SAND.KPIGenerator import BATRU_SANDGenerator

__author__ = 'uri'


class BATRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        BATRU_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '3a7c2d9d-d0bf-41bf-af6a-fae9892d66ce'
#     data_provider.load_session_data(session)
#     output = Output()
#     BATRUCalculations(data_provider, output).run_project_calculations()
