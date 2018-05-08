
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MARS_CHOCO_RU_SAND.KPIGenerator import MARS_CHOCO_RU_SANDMARSGenerator

__author__ = 'Sanad'


class MARS_CHOCO_RU_SANDMARSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARS_CHOCO_RU_SANDMARSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('mars-choco-ru calculations')
#     Config.init()
#     project_name = 'mars-choco-ru'
#     data_provider = KEngineDataProvider(project_name)
#     session = '90391040-7D32-4501-AB60-9D663FF336F6'
#     data_provider.load_session_data(session)
#     output = Output()
#     MARS_CHOCO_RU_SANDMARSCalculations(data_provider, output).run_project_calculations()
