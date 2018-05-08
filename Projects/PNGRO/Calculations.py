
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGRO.KPIGenerator import PNGRO_PRODGenerator

__author__ = 'Israel'


class PNGRO_PRODCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGRO_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngro calculations')
#     Config.init()
#     project_name = 'pngro'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'aaa45f15-27c4-4860-ac4f-fb2a83647b21'
#     # session = '0151e826-dee6-486d-a5f5-220d45360d9c'
#     # session = 'FB64C4E3-C672-4DBA-AF55-11FE3349D58F'
#     session = '43e837e9-3916-41a5-a4a0-51d31c8ae81f'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGRO_PRODCalculations(data_provider, output).run_project_calculations()

