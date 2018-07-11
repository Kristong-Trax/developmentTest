
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGJP.KPIGenerator import PNGJPGenerator

__author__ = 'Nimrod'


class PNGJPCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGJPGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('pngjp calculations')
    Config.init()
    project_name = 'pngjp'
    data_provider = KEngineDataProvider(project_name)
    session = 'CEF8E4D9-09CB-4641-9362-E7F56A49CD9C'
    # session = '22920B98-E56A-4CF5-BDC1-AF288FA0ED9B'
    data_provider.load_session_data(session)
    output = Output()
    PNGJPCalculations(data_provider, output).run_project_calculations()
