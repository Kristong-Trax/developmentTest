
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGRO.KPIGenerator import PNGRO_PRODGenerator

__author__ = 'Israel'


class PNGRO_PRODCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGRO_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngro calculations')
#     Config.init()
#     project_name = 'pngro'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         '5CC7FBE2-BD55-41B3-862C-ED5DD103E50F'
#         ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGRO_PRODCalculations(data_provider, output).run_project_calculations()

