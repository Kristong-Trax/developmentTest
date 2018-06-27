
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
#
#         '5CC7FBE2-BD55-41B3-862C-ED5DD103E50F',
#         'e732ac10-ee03-4a46-9cb8-88a85d2a57f9',
#         '0dc9f0b8-2844-4ebd-b429-92ca7c8f8dca',
#         'cb1d858c-5d57-42ab-93ab-625e25886ccb',
#         '4ed5187b-2ecf-498b-afd1-18d7bd61a93b',
#         '3829faec-c841-440d-af22-e9770532e57e'
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGRO_SAND_PRODCalculations(data_provider, output).run_project_calculations()

