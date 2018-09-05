
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOES.KPIGenerator import DIAGEOESGenerator

__author__ = 'Yasmin'


class DIAGEOESCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOESGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('diageoes calculations')
    Config.init()
    project_name = 'diageoes'
    data_provider = KEngineDataProvider(project_name)
    session = '065F07D9-B669-45E2-82D5-38C69C2AC9D6'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOESCalculations(data_provider, output).run_project_calculations()
