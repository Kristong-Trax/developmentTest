
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.CCUS_SAND.KPIGenerator import CCUSGenerator

__author__ = 'ortal'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ccus-sand calculations')
#     Config.init()
#     project_name = 'ccus-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session_uids = ['50d6ba7c-e0df-4cf6-8a71-4adc1865bac4','A7D0BCA3-6E21-43C7-B22A-01C74A4CEF67']
#     for session in session_uids:
#         data_provider.load_session_data(session)
#         output = Output()
#         CCUSCalculations(data_provider, output).run_project_calculations()
