
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOPT_SAND.KPIGenerator import DIAGEOPT_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOPT_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOPT_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageopt calculations')
#     Config.init()
#     project_name = 'diageopt-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C00F49D3-828C-475D-B71B-52B2583A6110'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOPT_SANDCalculations(data_provider, output).run_project_calculations()
