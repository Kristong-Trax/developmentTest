
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.INBEVBR_SAND.KPIGenerator import INBEVBR_SANDGenerator

__author__ = 'Yasmin'


class INBEVBR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBR_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbr-sand calculations')
#     Config.init()
#     project_name = 'inbevbr-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'a1521669-d851-4a17-ada9-9e8d95370c3f'
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVBR_SANDCalculations(data_provider, output).run_project_calculations()
