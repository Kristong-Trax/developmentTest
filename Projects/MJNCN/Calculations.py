
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MJNCN.KPIGenerator import MJNCNGenerator

__author__ = 'Yasmin'


class MJNCNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MJNCNGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('mjncn calculations')
#     Config.init()
#     project_name = 'mjncn'
#     data_provider = KEngineDataProvider(project_name)
#     session = '8D607C3E-9E91-4308-9345-29E774BA2C69'
#     data_provider.load_session_data(session)
#     output = Output()
#     MJNCNCalculations(data_provider, output).run_project_calculations()
