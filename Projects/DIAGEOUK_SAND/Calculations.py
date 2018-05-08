
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOUK_SAND.KPIGenerator import DIAGEOUK_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOUK_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOUK_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageouk-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '97E6CEEF-3150-434B-81A2-856765310C0E'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOUK_SANDCalculations(data_provider, output).run_project_calculations()
