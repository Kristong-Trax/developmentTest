
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from kpi_factory.Projects.DIAGEOUK.KPIGenerator import DIAGEOUKGenerator

__author__ = 'Nimrod'


class DIAGEOUKCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOUKGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageouk'
#     data_provider = KEngineDataProvider(project_name)
#     session = '5E50365B-8C97-4CBF-A6F2-341D4E1DA466'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOUKCalculations(data_provider, output).run_project_calculations()
