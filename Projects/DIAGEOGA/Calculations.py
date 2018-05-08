
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGA.KPIGenerator import DIAGEOGADIAGEOGAGenerator

__author__ = 'Yasmin'


class DIAGEOGADIAGEOGACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGADIAGEOGAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoga-sand calculations')
#     Config.init()
#     project_name = 'diageoga-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGADIAGEOGACalculations(data_provider, output).run_project_calculations()
