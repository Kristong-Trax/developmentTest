
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOAR.KPIGenerator import DIAGEOARDIAGEOARGenerator

__author__ = 'Yasmin'


class DIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOARDIAGEOARGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar calculations')
#     Config.init()
#     project_name = 'diageoar'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'df6add11-8fc6-4503-8e82-21d79e9870c0'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOARDIAGEOARCalculations(data_provider, output).run_project_calculations()
