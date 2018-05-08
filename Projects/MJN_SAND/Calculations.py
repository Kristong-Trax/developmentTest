
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MJN_SAND.KPIGenerator import MJNCN_SANDGenerator

__author__ = 'Yasmin'


class MJNCNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MJNCN_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('mjncn-sand calculations')
#     Config.init()
#     project_name = 'mjn-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '56BE48DA-451E-4ABA-A445-E0098C919868'
#     data_provider.load_session_data(session)
#     output = Output()
#     MJNCNCalculations(data_provider, output).run_project_calculations()
