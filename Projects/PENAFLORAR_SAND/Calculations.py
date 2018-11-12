
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PENAFLORAR_SAND.KPIGenerator import PENAFLORAR_SANDDIAGEOARGenerator

__author__ = 'Yasmin'


class PENAFLORAR_SANDDIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PENAFLORAR_SANDDIAGEOARGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar-sand calculations')
#     Config.init()
#     project_name = 'penaflorar-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '98918211-50f7-47c1-a722-fed4ae3ba7b8'
#     data_provider.load_session_data(session)
#     output = Output()
#     PENAFLORAR_SANDDIAGEOARCalculations(data_provider, output).run_project_calculations()
