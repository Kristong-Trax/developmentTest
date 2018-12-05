import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOMX.KPIGenerator import DIAGEOMXGenerator

__author__ = 'Nimrod'


class DIAGEOMXCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOMXGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx calculations')
#     Config.init()
#     project_name = 'diageomx'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b124681d-8865-410e-8b25-b7ebff404526'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOMXCalculations(data_provider, output).run_project_calculations()
