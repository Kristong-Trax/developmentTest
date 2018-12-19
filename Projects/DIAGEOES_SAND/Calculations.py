
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOES_SAND.KPIGenerator import DIAGEOESSANDGenerator

__author__ = 'Nimrod'


class DIAGEOESSANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOESSANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageoes-sand calculations')
#     Config.init()
#     project_name = 'diageoes-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'F9E5B557-84D2-4334-9814-6B972FA950AF'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOESSANDCalculations(data_provider, output).run_project_calculations()
