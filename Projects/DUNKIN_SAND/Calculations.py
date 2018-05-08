
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock

from Projects.DIAGEOAU.KPIGenerator import DIAGEOAUGenerator
from Projects.DUNKIN_SAND.KPIGenerator import CCUSGenerator

__author__ = 'ortal'


class DunkinDonutsCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('BCI calculations')
#     Config.init()
#     project_name = 'ccbottlersus-sand'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session_uids = ['ea86ac8f-af19-438e-a2cc-ee13f042a826',
#                     'e24668c3-5abe-4976-a89f-f42c46e49f93',
#                     'e9f58ebc-4421-4575-90fd-d752164026b8',
#                     'ba823122-5193-438b-a1c8-bd5f3a1bc12e']
#
#     for session in session_uids:
#         data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#         data_provider.load_session_data(session)
#         output = Output()
#         DunkinDonutsCalculations(data_provider, output).run_project_calculations()


