
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log
from Projects.CUBAU.KPIGenerator import CUBAUGenerator

__author__ = 'Shani'


class CUBAUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CUBAUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     Log.init('cubau calculations')
#     Config.init()
#     project_name = 'cubau'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['FFCC3673-858B-4B41-9644-B7C57DD4B441']
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         CUBAUCalculations(data_provider, output).run_project_calculations()
