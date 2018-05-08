
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEAPI_SAND.KPIGenerator import NESTLEAPIGenerator

__author__ = 'Ortal'


class NESTLEAPICalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        NESTLEAPIGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


#
# if __name__ == '__main__':
#     LoggerInitializer.init('nestleapi-sand calculations')
#     Config.init()
#     project_name = 'nestleapi-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'ade42e23-4c25-4cf9-944b-b0016bf46403'
#     data_provider.load_session_data(session)
#     output = Output()
#     NESTLEAPICalculations(data_provider, output).run_project_calculations()
