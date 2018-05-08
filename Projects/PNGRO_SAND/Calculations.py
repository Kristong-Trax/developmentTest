
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGRO_SAND.KPIGenerator import PNGROGenerator

__author__ = 'Israel'


class PNGROCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGROGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngro-sand calculations')
#     Config.init()
#     project_name = 'pngro-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '43e837e9-3916-41a5-a4a0-51d31c8ae81f'
#     # session = '0151e826-dee6-486d-a5f5-220d45360d9c'
#     # session = 'FB64C4E3-C672-4DBA-AF55-11FE3349D58F'
#     # session = '31176E9E-AFA2-486A-A9D8-18F6F8AE4BF6'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGROCalculations(data_provider, output).run_project_calculations()
