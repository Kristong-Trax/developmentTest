
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGJP_SAND.KPIGenerator import PNGJP_SANDGenerator

__author__ = 'Nimrod'


class PNGJP_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGJP_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngjp calculations')
#     Config.init()
#     project_name = 'pngjp_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'E5696CEB-A8F0-48E2-8881-58929F475CD1'
#     # session = '22920B98-E56A-4CF5-BDC1-AF288FA0ED9B'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGJP_SANDCalculations(data_provider, output).run_project_calculations()
