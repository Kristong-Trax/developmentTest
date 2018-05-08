
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGAU.KPIGenerator import PNGAUGenerator

__author__ = 'Nimrod'


class PNGAUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGAUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngau calculations')
#     Config.init()
#     project_name = 'pngau'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'D61ECC28-6FBF-4CA0-8E5D-6CB134BEA65D'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGAUCalculations(data_provider, output).run_project_calculations()
