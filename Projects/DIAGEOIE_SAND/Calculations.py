
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIE_SAND.KPIGenerator import DIAGEOIE_SANDGenerator

__author__ = 'Yasmin'


class DIAGEOIECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOIE_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoie-sand calculations')
#     Config.init()
#     project_name = 'diageoie-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '2c965f54-e29e-4319-a84b-bf1783fdd7f4'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOIECalculations(data_provider, output).run_project_calculations()
