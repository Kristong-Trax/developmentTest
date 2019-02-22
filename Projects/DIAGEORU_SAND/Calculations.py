
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEORU_SAND.KPIGenerator import DIAGEORUGenerator

__author__ = 'Nimrod'


class DIAGEORUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEORUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoru-sand calculations')
#     Config.init()
#     project_name = 'diageoru-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '4897B335-AE86-4A0A-AD37-2F6DD75FD6B8'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEORUCalculations(data_provider, output).run_project_calculations()
