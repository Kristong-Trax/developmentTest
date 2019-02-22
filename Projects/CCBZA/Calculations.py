
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBZA.KPIGenerator import CCBZA_Generator

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer



__author__ = 'natalyak'


class CCBZA_Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBZA_Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccbza calculations')
#     Config.init()
#     project_name = 'ccbza-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         # 'AD29338A-C2D9-4486-BD94-7B1E32224A11'
#         # 'A9202C15-05D8-40B0-920D-B9E1CC758B2B' # test L&T
#         # 'D80757BB-0B33-4E87-889F-38B2158EBC95' # test QSR
#         # '9bc7f6ff-9a26-427d-b0d9-5f4d1b08e779'
#         '24b18836-8fdb-4b85-8119-3e076ed6e520'
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCBZA_Calculations(data_provider, output).run_project_calculations()
