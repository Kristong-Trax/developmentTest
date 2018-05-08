
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGAU_SAND.KPIGenerator import PNGAU_SANDGenerator

__author__ = 'Nimrod'


class PNGAU_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGAU_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngjp calculations')
#     Config.init()
#     project_name = 'pngau_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'D61ECC28-6FBF-4CA0-8E5D-6CB134BEA65D'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGAU_SANDCalculations(data_provider, output).run_project_calculations()
