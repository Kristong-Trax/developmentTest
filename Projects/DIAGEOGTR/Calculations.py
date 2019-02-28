
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGTR.KPIGenerator import DIAGEOGTRDIAGEOGTRGenerator

__author__ = 'Yasmin'


class DIAGEOGTRDIAGEOGTRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGTRDIAGEOGTRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageogtr calculations')
#     Config.init()
#     project_name = 'diageogtr'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'e4e2bdbd-b884-4968-b2f2-e033e3efab91'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGTRDIAGEOGTRCalculations(data_provider, output).run_project_calculations()
