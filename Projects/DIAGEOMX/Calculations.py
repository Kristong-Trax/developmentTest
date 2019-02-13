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


if __name__ == '__main__':
    LoggerInitializer.init('diageomx calculations')
    Config.init()
    project_name = 'diageomx'
    data_provider = KEngineDataProvider(project_name)
    session = '8e5c105e-5457-4c50-a934-7324706c1c29'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOMXCalculations(data_provider, output).run_project_calculations()
