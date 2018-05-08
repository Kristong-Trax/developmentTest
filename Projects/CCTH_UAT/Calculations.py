
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCTH_UAT.KPIGenerator import CCTH_UATGenerator

__author__ = 'Nimrod'


class CCTH_UATCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCTH_UATGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccth calculations')
#     Config.init()
#     project_name = 'ccth-uat'
#     data_provider = KEngineDataProvider(project_name)
#     session = '6c1761b9-1298-4421-be90-f71752d0789d'
#     data_provider.load_session_data(session)
#     output = Output()
#     CCTH_UATCalculations(data_provider, output).run_project_calculations()
