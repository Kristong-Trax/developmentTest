
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVLU_SAND.KPIGenerator import INBEVLU_SANDINBEVBEGenerator

__author__ = 'urid'


class INBEVLU_SANDINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVLU_SANDINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevlu-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = "bea9c5cc-943d-49b2-85b8-c5a3d49ad9ec"
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVLU_SANDINBEVBECalculations(data_provider, output).run_project_calculations()
