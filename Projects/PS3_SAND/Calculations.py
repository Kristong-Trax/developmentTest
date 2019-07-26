import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PS3_SAND.KPIGenerator import PS3_SANDGenerator

__author__ = 'Nimrod'


class PS3_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PS3_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx calculations')
#     Config.init()
#     project_name = 'ps3_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'fd7d2a19-3a1c-40fd-a7d1-3a01260392d1'
#     data_provider.load_session_data(session)
#     output = Output()
#     PS3_SANDCalculations(data_provider, output).run_project_calculations()
