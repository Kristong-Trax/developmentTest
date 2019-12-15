
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCZA.KPIGenerator import CCZAGenerator

__author__ = 'Elyashiv'


class CCZACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCZAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccza calculations')
#     Config.init()
#     project_name = 'ccza'
#     sessions = [
#                 # '50cf705c-bf8f-4eff-a813-6aa7c0835132', # missing survey codes
#                 # 'FF39172D-EE62-4184-ABF7-91CD2D05800B',
#                 # 'FE8E0E52-9DF1-40BF-81E2-C6F618682806',
#                 # 'FE1CF09E-272A-4B15-9BF4-E9E4228A2BF2'
#                 'F031B961-12D6-4648-A22B-7F13872799DE' # wholesaler for new kpis
#     ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         print session
#         data_provider.load_session_data(session)
#         output = Output()
#         CCZACalculations(data_provider, output).run_project_calculations()
