
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.RBUS_SAND.KPIGenerator import RBUSGenerator

__author__ = 'yoava'


class RBUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RBUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
# if __name__ == '__main__':
#     LoggerInitializer.init('rbus-sand calculations')
#     Config.init()
#     project_name = 'rbus-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b596a3fb-541b-11e8-b95f-0a0d2137d3d2'
#     data_provider.load_session_data(session)
#     output = Output()
#     RBUSCalculations(data_provider, output).run_project_calculations()
