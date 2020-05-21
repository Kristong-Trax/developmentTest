
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PS1_SAND.KPIGenerator import DIAGEOTWGenerator

__author__ = 'Nimrod'


class DIAGEOTWCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOTWGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('diageotw calculations')
    Config.init()
    project_name = 'diageotw'
    data_provider = KEngineDataProvider(project_name)
    session = '2B9749B4-9AE9-4676-A389-2BB3C83029BC'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOTWCalculations(data_provider, output).run_project_calculations()
