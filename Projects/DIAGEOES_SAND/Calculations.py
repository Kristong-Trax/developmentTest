
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.DIAGEOES_SAND.KPIGenerator import DIAGEOESSANDGenerator

__author__ = 'Nimrod'


class DIAGEOESSANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOESSANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('diageoes-sand calculations')
#     Config.init()
#     project_name = 'diageoes-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C7D5017A-0B50-4D27-814F-59405CACD471'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOESSANDCalculations(data_provider, output).run_project_calculations()
