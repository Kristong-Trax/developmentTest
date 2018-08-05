
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBZA_SAND.KPIGenerator import CCBZA_SANDGenerator

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'natalyak'


class CCBZA_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBZA_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('ccbza-sand calculations')
    Config.init()
    project_name = 'ccbza-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'AD29338A-C2D9-4486-BD94-7B1E32224A11'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        CCBZA_SANDCalculations(data_provider, output).run_project_calculations()
