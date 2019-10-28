
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CBCIL_SAND.KPIGenerator import CBCIL_PRODGenerator

__author__ = 'Israel'


class CBCIL_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CBCIL_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
#
# if __name__ == '__main__':
#     LoggerInitializer.init('cbcil calculations')
#     Config.init()
#     project_name = 'cbcil-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#
#        '1011c545-40c5-4081-ae79-4f6a7124bc2b',
#         '7ff3411e-0d6c-472e-a336-a7702b36986e',
#         'b873b2e8-caee-40a4-9498-cc2cbba80e27',
#         '44b15455-0f77-4dfe-b79d-f002125d820d',
#         'b4cb5840-df3f-46d9-a5b2-d6df9645ba21',
#         '9c51bb6e-221e-4d95-8884-9c1da429a83c'
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CBCIL_SANDCalculations(data_provider, output).run_project_calculations()
#
