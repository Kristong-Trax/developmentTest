
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.BATRU_SAND.KPIGenerator import BATRU_SANDGenerator
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = 'uri'


class BATRU_SANDCalculations(BaseCalculationsScript):

    @log_runtime('Total Calculations', log_start=True)
    def run_project_calculations(self):
        self.timer.start()
        BATRU_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('batru calculations')
#     Config.init()
#     project_name = 'batru-sand'
#     sessions = [
#          '33333B06-474E-4259-BF22-6A9064374E9D']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         BATRU_SANDCalculations(data_provider, output).run_project_calculations()
