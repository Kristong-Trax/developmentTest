
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MARSIN.KPIGenerator import MARSINGenerator

__author__ = 'Nimrod'


class MARSINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARSINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('marsin calculations')
#     Config.init()
#     project_name = 'marsin'
#     data_provider = KEngineDataProvider(project_name)
#     session = '951e9ee9-8c20-4dc6-8bc3-84dcdac79988'
#     data_provider.load_session_data(session)
#     output = Output()
#     MARSINCalculations(data_provider, output).run_project_calculations()
