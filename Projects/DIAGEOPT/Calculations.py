
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOPT.KPIGenerator import DIAGEOPTGenerator

__author__ = 'Nimrod'


class DIAGEOPTCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOPTGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageopt calculations')
#     Config.init()
#     project_name = 'diageopt'
#     data_provider = KEngineDataProvider(project_name)
#     session = '963D013D-EEB6-48DF-B8EC-06C8E0C2AA6C'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOPTCalculations(data_provider, output).run_project_calculations()
