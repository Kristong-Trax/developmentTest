
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CCPH.KPIGenerator import Generator
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output

__author__ = 'satya'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')