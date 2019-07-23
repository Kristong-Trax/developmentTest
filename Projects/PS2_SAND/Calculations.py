
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PS2_SAND.KPIGenerator import DIAGEOPLGenerator

__author__ = 'Nimrod'


class DIAGEOPLCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOPLGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageopl calculations')
#     Config.init()
#     project_name = 'ps2-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '8400a292-4b23-4895-8f1e-b1597a7335b3'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOPLCalculations(data_provider, output).run_project_calculations()
