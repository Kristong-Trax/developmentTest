
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVLU.KPIGenerator import INBEVLUINBEVBEGenerator

__author__ = 'urid'


class INBEVLUINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVLUINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevlu calculations')
#     Config.init()
#     project_name = 'inbevlu'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = "9a5a162a-e84e-4d6d-9413-ed6c6f8b43fd"
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVLUINBEVBECalculations(data_provider, output).run_project_calculations()
