
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.JTIUA.KPIGenerator import JTIUAGenerator

__author__ = 'Nimrod'


class JTIUACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        JTIUAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('jtiua calculations')
#     Config.init()
#     project_name = 'jtiua'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'a88557dd-ba1c-4571-a4b3-b022f1388152'
#     data_provider.load_session_data(session)
#     output = Output()
#     JTIUACalculations(data_provider, output).run_project_calculations()
