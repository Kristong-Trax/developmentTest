
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGR.KPIGenerator import DIAGEOGRGenerator

__author__ = 'Nimrod'


class DIAGEOGRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'diageogr'
    data_provider = KEngineDataProvider(project_name)
    session = '8180ec3c-6297-45af-be05-9ae081a30b04'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOGRCalculations(data_provider, output).run_project_calculations()
