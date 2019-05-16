import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock

from Projects.INBEVNL.KPIGenerator import INBEVNLINBEVBEGenerator

__author__ = 'urid'


class INBEVNLINBEVBECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'INBEVNL', 'Data',
                                     'POCE_Template.xlsx')
        INBEVNLINBEVBEGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('inbevnl calculations')
    Config.init()
    project_name = 'inbevnl'
    data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
    session = 'f1d066a4-ba8a-4af2-957d-7a538096ab4d'
    data_provider.load_session_data(session)
    output = Output()
    INBEVNLINBEVBECalculations(data_provider, output).run_project_calculations()
