
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGR_SAND.KPIGenerator import DIAGEOGR_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOGR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGR_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageogr-sand'
#     data_provider = ACEDataProvider(project_name)
#     session = '03573352-467F-4C73-BBFE-60FCB8A04B2C'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGR_SANDCalculations(data_provider, output).run_project_calculations()
