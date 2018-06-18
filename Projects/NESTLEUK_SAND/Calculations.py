
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUK_SAND.KPIGenerator import NESTLEUK_SANDGenerator

__author__ = 'uri'


class NESTLEUK_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        NESTLEUK_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('nestleuk calculations')
#     Config.init()
#     project_name = 'nestleuk_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '82b0fb4f-f754-428d-83cb-b39b3c184c4c'
#     data_provider.load_session_data(session)
#     output = Output()
#     NESTLEUK_SANDCalculations(data_provider, output).run_project_calculations()
