
# from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from mock import MagicMock
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS_SAND.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')



# if __name__ == '__main__':
#     LoggerInitializer.init('altria-sand calculations')
#     Config.init()
#     project_name = 'altriaus'
#     sessions = ['7c46c139-cbfd-11e8-8a81-129f596660d8']
#
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()