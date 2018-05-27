
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MARSIN_SAND.KPIGenerator import MARSIN_SANDGenerator

__author__ = 'Nimrod'


class MARSIN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARSIN_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     Log.init('marsin calculations')
#     Config.init()
#     project_name = 'marsin-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'faca16aa-cf53-4e7c-a4a1-9b9ad5b77ad6'
#     data_provider.load_session_data(session)
#     output = Output()
#     MARSIN_SANDCalculations(data_provider, output).run_project_calculations()
