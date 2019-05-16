
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOPL_SAND.KPIGenerator import DIAGEOPL_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOPL_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOPL_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageopl-sand calculations')
#     Config.init()
#     project_name = 'diageopl-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '963D013D-EEB6-48DF-B8EC-06C8E0C2AA6C'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOPL_SANDCalculations(data_provider, output).run_project_calculations()
