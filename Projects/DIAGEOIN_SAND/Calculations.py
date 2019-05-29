
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN_SAND.KPIGenerator import DIAGEOIN_SANDGenerator


__author__ = 'nidhin'


class DIAGEOIN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOIN_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#         LoggerInitializer.init('DIAGEOIN_SAND calculations')
#         Config.init()
#         project_name = 'diageoin-sand'
#         data_provider = KEngineDataProvider(project_name)
#         sessions = [
#             'fc4035b8-1ecd-4b1a-b7f5-dc594001d957',
#             # '8a1164b9-5634-478d-a30a-b31f10d52cb8',
#             # '6fe43ddf-2603-49a4-aad8-e6f5c8da399b',
#         ]
#         for session in sessions:
#             data_provider.load_session_data(session)
#             output = Output()
#             DIAGEOIN_SANDCalculations(data_provider, output).run_project_calculations()
