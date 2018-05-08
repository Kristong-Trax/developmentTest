
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVNL_SAND.KPIGenerator import INBEVNL_SANDINBEVBEGenerator

__author__ = 'urid'


class INBEVNL_SANDINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVNL_SANDINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevnl-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = 'F2EC07FB-3863-4337-8764-1BE5F8A573B2'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVNL_SANDINBEVBECalculations(data_provider, output).run_project_calculations()
