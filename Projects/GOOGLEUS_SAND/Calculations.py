
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.GOOGLEUS_SAND.KPIGenerator import GOOGLEUS_SANDGenerator

__author__ = 'Ortal'


class GOOGLEUS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GOOGLEUS_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('googleus calculations')
#     Config.init()
#     project_name = 'googleus-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0e8a9541-c32b-4114-a777-9ce6677dec8a'
#     data_provider.load_session_data(session)
#     output = Output()
#     GOOGLEUS_SANDCalculations(data_provider, output).run_project_calculations()
