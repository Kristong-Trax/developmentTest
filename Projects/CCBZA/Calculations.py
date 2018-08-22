
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.CCBZA.KPIGenerator import CCBZA_Generator

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer



__author__ = 'natalyak'


class CCBZA_Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBZA_Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('ccbza calculations')
    Config.init()
    project_name = 'ccbza'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 'AD29338A-C2D9-4486-BD94-7B1E32224A11'
        '294F4764-4BA2-4243-A8CF-52FA32497BC4'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        CCBZA_Calculations(data_provider, output).run_project_calculations()
