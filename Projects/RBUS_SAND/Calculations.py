
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config

from Projects.RBUS_SAND.KPIGenerator import RBUSGenerator

__author__ = 'yoava'


class RBUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RBUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('rbus-sand calculations')
#     Config.init()
#     project_name = 'rbus-sand'
#     data_provider = KEngineDataProvider(project_name)
#     # session = '537C4629-9347-473D-8162-8BBB47E9482D'
#     session = 'b6ce555b-763d-404a-b1d7-b514f3bb9bae'
#     data_provider.load_session_data(session)
#     output = Output()
#     RBUSCalculations(data_provider, output).run_project_calculations()
