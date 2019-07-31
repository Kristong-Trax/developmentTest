
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.MARSIN_SAND.KPIGenerator import MARSIN_SANDGenerator

__author__ = 'Nimrod'


class MARSIN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MARSIN_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('marsin-sand calculations')
    project_name = 'marsin-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '1e8e2782-a406-4da8-8bef-864cc30cc1b1'
    data_provider.load_session_data(session)
    output = Output()
    MARSIN_SANDCalculations(data_provider, output).run_project_calculations()
