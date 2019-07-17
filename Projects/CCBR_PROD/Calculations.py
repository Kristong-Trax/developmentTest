
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.CCBR_PROD.KPIGenerator import CCBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class CCBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('ccbr-prod calculations')
#     Config.init()
#     project_name = 'ccbr-prod'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#     list_sessions = ['8cc8c098-fa97-47dd-83d6-00f109a78ada']
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         CCBRCalculations(data_provider, output).run_project_calculations()

