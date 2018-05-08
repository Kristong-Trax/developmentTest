
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUK.KPIGenerator import NESTLEUKGenerator

__author__ = 'uri'


class NESTLEUKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        NESTLEUKGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('nestleuk calculations')
#     Config.init()
#     project_name = 'nestleuk'
#     data_provider = KEngineDataProvider(project_name)
#     session = '82b0fb4f-f754-428d-83cb-b39b3c184c4c'
#     data_provider.load_session_data(session)
#     output = Output()
#     NESTLEUKCalculations(data_provider, output).run_project_calculations()
