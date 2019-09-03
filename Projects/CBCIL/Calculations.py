
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CBCIL.KPIGenerator import CBCILCBCIL_PRODGenerator

__author__ = 'Israel'


class CBCILCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CBCILCBCIL_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('cbcil calculations')
#     Config.init()
#     project_name = 'cbcil'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         'a0bac8a6-5cbb-4843-a582-b042b42a42f6'
#         # '95513285-4c78-4c91-85dd-cae2ff83bbf5' #tested and changed
#         # 'a0ba8c7d-7dec-40c4-9730-eaea3edcfc95' # tested
#         # '4bc4c803-c1cb-4a0f-a753-3da1b7f94216' # tested
#         # '9a8f1598-ca3b-4cf6-a952-c7d025d5c517'# tested
#         # 'd12393f4-0a76-4959-9d10-40453ead32d0' # tested
#         # '5059e40d-19ed-4ade-ae0d-92ef9c9b0ff7' #tested
#         # '7e752f99-a079-41d2-bcef-850c634656ea',
#         # '9a8f1598-ca3b-4cf6-a952-c7d025d5c517',
#     ]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CBCILCalculations(data_provider, output).run_project_calculations()

