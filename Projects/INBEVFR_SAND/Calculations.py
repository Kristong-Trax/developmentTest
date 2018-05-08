
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVFR_SAND.KPIGenerator import INBEVFR_SAND_PRODINBEVBEGenerator

__author__ = 'urid'


class INBEVFR_SAND_PRODINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVFR_SAND_PRODINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevbe-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = '6932c6ca-523e-42fe-aff3-1c891f8b9e0e'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVFR_SAND_PRODINBEVBECalculations(data_provider, output).run_project_calculations()
