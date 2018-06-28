
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGRO_SAND.KPIGenerator import PNGRO_SAND_PRODGenerator

__author__ = 'Israel'


class PNGRO_SAND_PRODCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGRO_SAND_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngro calculations')
#     Config.init()
#     project_name = 'pngro-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         # 'cc0385ba-93b6-4d9f-808d-393936a84fac',
#         # '82de15ca-23f4-43a7-b22b-88fcd9948fb6',
#         # '2D4BB2DF-1CD5-4B82-968D-239DFA6090E9',
#         # 'c2eb28bd-e200-44a2-8a39-c50ead41d5e0',
#         # '3ed3ffa7-0024-42fd-9124-9206d7702fc8',
#         # '9ff1c105-4a93-4f58-8064-a966a48b0942',
#         '5dcdc4ea-55be-432f-9815-33fcfbb4ab02',
#         '1a0a159e-a0ff-4e8d-a784-07609dfca8cd',
#         '0f92d689-2e7b-4351-8ab9-29dc59eb865d',
#         'd50f2225-4779-4b81-b806-a425e59cbd7c',
#         'c56b9f18-703f-43cc-8b61-c2b6e3ff1fae',
#         'adc69ee2-3d24-41aa-a864-b184e92a5a9d',
#         '73e0be3d-aada-4c99-bf4e-37667e668906',
#         'ff3f46b4-954a-4ef1-9ebc-f67c676cc2d5'
#
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGRO_SAND_PRODCalculations(data_provider, output).run_project_calculations()

