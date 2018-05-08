
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBR_SAND.KPIGenerator import DIAGEOBR_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOBR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOBR_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageobr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '26cec81a-3c3d-11e7-8182-12fe516ecfce'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOBR_SANDCalculations(data_provider, output).run_project_calculations()
