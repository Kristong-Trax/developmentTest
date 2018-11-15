
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config

from Projects.RBUS.KPIGenerator import RBUSRBUSGenerator

__author__ = 'yoava'


class RBUSRBUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RBUSRBUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('rbus-sand calculations')
#     Config.init()
#     project_name = 'rbus'
#     data_provider = KEngineDataProvider(project_name)
#     session = '47202937-9574-4BCC-B866-FC8EFA7FFD39'
#     data_provider.load_session_data(session)
#     output = Output()
#     RBUSRBUSCalculations(data_provider, output).run_project_calculations()
