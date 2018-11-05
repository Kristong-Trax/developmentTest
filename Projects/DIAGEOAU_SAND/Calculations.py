
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.DIAGEOAU_SAND.KPIGenerator import DIAGEOAU_SANDGenerator
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'Nimrod'


class DIAGEOAU_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOAU_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageoau-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '1C5D5006-AD55-40C3-9737-B500D90D407F'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOAU_SANDCalculations(data_provider, output).run_project_calculations()
