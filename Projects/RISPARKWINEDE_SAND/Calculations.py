
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.RISPARKWINEDE_SAND.KPIGenerator import RISPARKWINEDEGenerator

__author__ = 'nissand'


class RISPARKWINEDECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RISPARKWINEDEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('risparkwinede-sand calculations')
#     Config.init()
#     project_name = 'risparkwinede-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'B89D5121-1AB5-46AE-9B80-6EA7D1C0AB7F'
#     data_provider.load_session_data(session)
#     output = Output()
#     RISPARKWINEDECalculations(data_provider, output).run_project_calculations()
