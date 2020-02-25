
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCZA_SAND.KPIGenerator import CCZAGenerator

__author__ = 'Elyashiv'


class CCZACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCZAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccza-sand calculations')
#     Config.init()
#     project_name = 'ccza-sand'
#     sessions = ['f3ebbe88-41eb-43fc-b84d-4ba291b2873d', '4496dc92-9cb8-42d4-baeb-f90e0eb8cbaf', '1fd078bf-7172-4ee5-a252-d3a14408629b']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         print session
#         data_provider.load_session_data(session)
#         output = Output()
#         CCZACalculations(data_provider, output).run_project_calculations()
