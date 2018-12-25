
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
# #
# if __name__ == '__main__':
#     LoggerInitializer.init('pngro calculations')
#     Config.init()
#     project_name = 'pngro'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         '5bdaf070-8916-4459-bcbd-36473e37207b',
#         # '42FF3F61-87AF-45FE-8389-CF8E5FA600BB',
#         # '43061462-282C-47D4-BDB2-AE15C9413171',
#         # '659cbb3d-5e2f-44a2-81ed-7dd68dfb8d7d',
#         # 'a8fa4858-4f62-49a5-9b88-d4947d6da0b0',
#         # 'aaa45f15-27c4-4860-ac4f-fb2a83647b21'
#         ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGRO_PRODCalculations(data_provider, output).run_project_calculations()
#
