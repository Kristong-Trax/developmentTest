
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# # from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# # from Trax.Utils.Conf.Configuration import Config
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Utils.Logging.Logger import Log

from Projects.MILLERCOORS.KPIGenerator import MILLERCOORSGenerator

__author__ = 'Eli'


class MILLERCOORSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MILLERCOORSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('millercoors calculations')
#     Config.init()
#     project_name = 'millercoors'
#     data_provider = KEngineDataProvider(project_name)
#     session = '56110CA1-4C2A-4652-BDCB-463CCDA65AB6'
#     data_provider.load_session_data(session)
#     output = Output()
#     MILLERCOORSCalculations(data_provider, output).run_project_calculations()
