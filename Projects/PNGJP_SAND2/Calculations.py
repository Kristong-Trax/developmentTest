
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGJP_SAND2.KPIGenerator import PNGJP_SAND2Generator

__author__ = 'Nimrod'


class PNGJP_SAND2Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGJP_SAND2Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
#
# if __name__ == '__main__':
#     LoggerInitializer.init('pngjp calculations')
#     Config.init()
#     project_name = 'pngjp-sand2'
#     data_provider = KEngineDataProvider(project_name)
#     # session = '2A753CD4-4CBF-4903-9893-AC10FC42CD51'
#     # session = '22920B98-E56A-4CF5-BDC1-AF288FA0ED9B'
#     # session = 'A4C4B54E-A94A-4CCF-A5A9-5BCE7F045A71'
#     # session = 'f73fa07e-8891-4a64-bc76-5b8c9489ac60'
#     session = '9f67e612-fd0c-4115-836e-07718eadfbd5'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGJP_SAND2Calculations(data_provider, output).run_project_calculations()
