
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVNL.KPIGenerator import INBEVNLINBEVBEGenerator

__author__ = 'urid'


class INBEVNLINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVNLINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevnl calculations')
#     Config.init()
#     project_name = 'inbevnl'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = '231e2358-824e-4bf9-bd9d-281fc64dd093'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVNLINBEVBECalculations(data_provider, output).run_project_calculations()
