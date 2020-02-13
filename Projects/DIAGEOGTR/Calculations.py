
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
#     sessions = ["3A258DEB-D7E5-418C-A982-C59F3FBB6076"]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOGTRCalculations(data_provider, output).run_project_calculations()
