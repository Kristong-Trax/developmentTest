
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSRS.KPIGenerator import MOLSONCOORSRSGenerator

__author__ = 'sergey'


class MOLSONCOORSRSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MOLSONCOORSRSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIMOLSONCOORSRSGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('molsoncoorshr calculations')
#     Config.init()
#     project_name = 'molsoncoorsrs'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#                 '9a06e981-ce48-4619-9eba-0969490537f3',
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         MOLSONCOORSRSCalculations(data_provider, output).run_project_calculations()
#
#

