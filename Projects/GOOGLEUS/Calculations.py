
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.GOOGLEUS.KPIGenerator import GOOGLEUSGenerator

__author__ = 'Ortal'


class GOOGLEUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        GOOGLEUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('googleus calculations')
#     Config.init()
#     project_name = 'googleus'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0e8a9541-c32b-4114-a777-9ce6677dec8a'
#     data_provider.load_session_data(session)
#     output = Output()
#     GOOGLEUSCalculations(data_provider, output).run_project_calculations()
