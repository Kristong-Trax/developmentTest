
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'satya'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('DIAGEOIN calculations')
    Config.init()
    project_name = 'diageoin'
    data_provider = KEngineDataProvider(project_name)
    session = 'f6463d98-5e1c-44e8-a571-8999b79e5e3d'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOINCalculations(data_provider, output).run_project_calculations()