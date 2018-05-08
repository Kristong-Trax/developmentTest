from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from mock import MagicMock

from Projects.INBEVBE.KPIGenerator import INBEVBEINBEVBEGenerator

__author__ = 'urid'


class INBEVBEINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBEINBEVBEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbe calculations')
#     Config.init()
#     project_name = 'inbevbe'
#     data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
#     session = '65fe0dd4-f2d3-4d08-a4e1-f18073b930bd'
#     # 1ac5630f-f5fa-4932-a1b8-82b8100e4106
#     data_provider.load_session_data(session)
#     output = Output()
#     INBEVBEINBEVBECalculations(data_provider, output).run_project_calculations()
