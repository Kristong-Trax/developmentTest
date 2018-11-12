
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOTW_SAND.KPIGenerator import DIAGEOTW_SANDGenerator

__author__ = 'Nimrod'


class DIAGEOTW_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOTW_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageotw-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '07EDFA13-412C-4A5E-B7B1-CA4B7E3E06A5'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOTW_SANDCalculations(data_provider, output).run_project_calculations()
