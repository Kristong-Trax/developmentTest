
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU.KPIGenerator import BATRUGenerator
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'uri'


class BATRUCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        BATRUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru'
#     # data_provider = KEngineDataProvider(project_name)
#     # session = '78a6aaba-c5d6-4dd7-9fbb-8423792af0d1'
#     # data_provider.load_session_data(session)
#     # output = Output()
#     # BATRUCalculations(data_provider, output).run_project_calculations()
#     sessions = [
#                 # 'fa5d21e8-457f-40ae-8d6e-1dbf66febcb7',
#                 # 'ffa70f4e-d934-412f-855f-0c2b6465c07b',
#                 # 'FF96883A-43C8-4BD8-A307-5F8DE5C2DD86',
#                 'ffed8c38-8182-4e89-bbdc-0ea88e3b3eaf',
#                 'FF27F8F5-A994-4E65-82C2-26ACED5CCB3A',
#                 ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRUCalculations(data_provider, output).run_project_calculations()
