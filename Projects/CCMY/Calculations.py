
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCMY.KPIGenerator import CCMYGenerator

__author__ = 'Nimrod'


class CCMYCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCMYGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('ccmy-sand calculations')
#     Config.init()
#     project_name = 'ccmy-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions =['4689b9e7-baf5-4ceb-961e-ab4b2bc61bd5','1036e8cf-80e3-4044-a871-00ebf3d87007','1a579151-b2af-4952-a3f0-09a3d4724f24',
#                '524e1fff-d7b4-4947-bafb-403f2ca04da6','05f623c3-a0ba-4b6c-9fbe-3a7703f5dd75']
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCMYCalculations(data_provider, output).run_project_calculations()