
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVTRADMX.KPIGenerator import Generator

__author__ = 'yoava'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevtradmx calculations')
#     Config.init()
#     project_name = 'inbevtradmx'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'F61379E6-0754-45E1-B987-721A6E800787'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
