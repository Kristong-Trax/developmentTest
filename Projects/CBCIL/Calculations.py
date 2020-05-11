
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CBCIL.KPIGenerator import CBCILPRODGenerator

__author__ = 'Israel'


class CBCILCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CBCILPRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('cbcil calculations')
#     Config.init()
#     project_name = 'cbcil'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         'fef668fb-b776-45e8-8df8-87ae8ade5f3c',
#         # '9a8f1598-ca3b-4cf6-a952-c7d025d5c517',
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CBCILCalculations(data_provider, output).run_project_calculations()
#
