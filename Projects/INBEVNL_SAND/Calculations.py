import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVNL_SAND.KPIGenerator import INBEVNL_SANDINBEVBEGenerator


__author__ = 'urid'


class INBEVNL_SANDINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'INBEVNL_SAND', 'Data',
                                     'POCE_Template.xlsx')
        INBEVNL_SANDINBEVBEGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevnl-sand calculations')
#     Config.init()
#     project_name = 'inbevnl-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'e910080a-c545-4d35-8589-3bd9614012fc'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVNL_SANDINBEVBECalculations(data_provider, output).run_project_calculations()
