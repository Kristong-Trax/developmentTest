
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock

from Projects.INBEVBE_SAND.KPIGenerator import INBEVBE_SANDGenerator

__author__ = 'urid'


class INBEVBE_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBE_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevbe-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session_uids = ['c0d4372f-72e7-46c6-8b53-f856a6393dd2']
#                     # '76635bc2-37b0-43f7-9c0a-b423b6e8f319', a874f6d5-c397-49b0-9759-a7ab2aaa0d40
#                     # 'e7a8233a-5569-43ee-b9f8-a1202c5d1f25',
#                     # 'd761d66a-9986-48a0-a67d-cab5b6015e8d',
#                     # 'cda8f651-442b-4a30-9d77-f1b722fc3575']
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         INBEVBE_SANDCalculations(data_provider, output).run_project_calculations()
