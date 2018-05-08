
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVFR.KPIGenerator import INBEVFR_PRODINBEVBEGenerator

__author__ = 'urid'


class INBEVFR_PRODINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVFR_PRODINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevfr'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     sessions = ['c893650c-3abe-4040-9452-f3bf53ea798a']
#         # , 'd2ec8cfb-c638-42e3-8ac7-a28e217520e5', '0c178fac-5f2b-4166-b598-bc467439eb82', '81b44f62-2d73-4c1d-b6db-3775bf144ed3']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVFR_PRODINBEVBECalculations(data_provider, output).run_project_calculations()
