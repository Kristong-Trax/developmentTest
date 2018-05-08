
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.RINIELSENUS_OLD.KPIGenerator import RINIELSENUSGenerator

__author__ = 'uri'


class RINIELSENUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RINIELSENUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('rinielsenus calculations')
#     Config.init()
#     project_name = 'rinielsenus'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     RINIELSENUSCalculations(data_provider, output).run_project_calculations()
