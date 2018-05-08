
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PEPSICOBR.KPIGenerator import PEPSICOBRGenerator

__author__ = 'Nimrod'


class PEPSICOBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PEPSICOBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('pepsicobr calculations')
#     Config.init()
#     project_name = 'pepsicobr'
#     data_provider = KEngineDataProvider(project_name)
#     session = '14f2baae-b8a5-4a81-b5fc-10caf291b3c4'
#     data_provider.load_session_data(session)
#     output = Output()
#     PEPSICOBRCalculations(data_provider, output).run_project_calculations()
