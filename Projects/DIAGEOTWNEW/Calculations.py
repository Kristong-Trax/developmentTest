
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator

__author__ = 'Nimrod'


class DIAGEOTWNEWCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGenerator(self.data_provider, self.output).diageo_global_main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageotw calculations')
#     Config.init()
#     project_name = 'diageotw-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '6870BA3E-9BFC-468D-885C-281488BD3625'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOTWNEWCalculations(data_provider, output).run_project_calculations()
