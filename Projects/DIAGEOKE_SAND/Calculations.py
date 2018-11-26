
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOKE_SAND.KPIGenerator import DIAGEOKE_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOKE_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOKE_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoke-sand calculations')
#     Config.init()
#     project_name = 'diageoke-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '7c301f16-6384-4d4f-83a6-fa1a6b46c26d'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOKE_SANDCalculations(data_provider, output).run_project_calculations()
