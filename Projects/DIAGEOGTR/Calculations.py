
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGTR.KPIGenerator import DIAGEOGTRGenerator

__author__ = 'satya'


class DIAGEOGTRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGTRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     project_name = 'diageogtr'
#     LoggerInitializer.init(project_name + ' calculations')
#     Config.init()
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ["3A258DEB-D7E5-418C-A982-C59F3FBB6076",
#                 "953e2384-2cd1-422b-b0d0-66b6490eba21",
#                 "7C43B8A9-18D5-484E-A728-EB7CBD5C3D78",
#                 "4f9fdc4d-aafe-4e05-96f8-ddc99b1a2c61",
#                 "940C60E5-C5FE-4E9D-855D-8DD18EA6D047",
#                 "8c63ee95-2a03-46be-9b0f-aafcd4e2af1f",
#                 "295E91B4-FD1B-457E-AA4E-913167378B86",
#                 "1C116C38-E0D8-4642-A1CC-D368BEFF162E",
#                 "6b9c4a93-72de-44ed-bc69-db59fc31c35c",
#                 "6EA12BD7-CC9B-42A1-BE09-A117D761D794"]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOGTRCalculations(data_provider, output).run_project_calculations()
