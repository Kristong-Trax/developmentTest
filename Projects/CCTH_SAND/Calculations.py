
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCTH_SAND.KPIGenerator import CCTH_SANDGenerator

__author__ = 'Nimrod'


class CCTH_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCTH_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccth calculations')
#     Config.init()
#     project_name = 'ccth-sand'
#     data_provider = KEngineDataProvider(project_name)
#     # TT
#     sessions = \
#         [
#         # TT
#             # food shop
#             # '0bd3db85-92cd-40f8-b317-6f1b5cebd22b',
#             # '7d6cacdd-314f-435e-84d9-cd85ed37c748',
#             # # Provision Shop
#             # '196530EF-5321-41A2-8238-1D2F160BFAAF',
#             # 'f75bed3c-fa2a-4a58-b238-44287b09b5fb',
#
#         # 7-11
#         #     '8BF68759-946B-4C8C-BB0B-AD2764A08F9E',
#         #     '2e6859c4-e772-4801-b7b4-2f3220efb06e',
#             '287d4e41-b495-42bc-8fe1-d080fd5f4b30',
#         #     'a7e2a36d-cd75-4dbb-930d-a6c7d999932a',
#         #     '3cfee562-8162-45a6-8e95-d727fde4c3a5'
#         ]
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCTH_SANDCalculations(data_provider, output).run_project_calculations()
