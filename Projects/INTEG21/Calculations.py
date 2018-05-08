
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INTEG21.KPIGenerator import INTEG21INBEVBEGenerator

__author__ = 'urid'


class INTEG21INBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INTEG21INBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevbe-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = '18171ccf-25bc-4b4e-a856-8cd2d3db5f38'
#     data_provider.load_session_data(session)
#     output = Output()
#     INTEG21INBEVBECalculations(data_provider, output).run_project_calculations()
