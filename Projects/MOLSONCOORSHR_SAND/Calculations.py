
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSHR_SAND.KPIGenerator import MOLSONCOORSHR_SANDGenerator

__author__ = 'sergey'


class MOLSONCOORSHR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MOLSONCOORSHR_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIMOLSONCOORSHR_SANDGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('molsoncoorshr calculations')
#     Config.init()
#     project_name = 'molsoncoorshr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#                 'F3586DC2-D078-4011-8847-8300FC5EEBE2',
#                 '8A5A43AF-2DB6-4032-B7C1-E60FE51F6502',
#                 'A96ADB9E-F879-4397-B178-CFE45B9FE2A4',
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         MOLSONCOORSHR_SANDCalculations(data_provider, output).run_project_calculations()



