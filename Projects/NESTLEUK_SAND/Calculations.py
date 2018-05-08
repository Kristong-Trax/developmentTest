
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
#     project_name = 'nestleuk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '368269fa-3f4c-4199-b98c-f133e1bf9514'
#     data_provider.load_session_data(session)
#     output = Output()
#     NESTLEUK_SANDCalculations(data_provider, output).run_project_calculations()
