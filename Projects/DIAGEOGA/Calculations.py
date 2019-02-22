
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGA.KPIGenerator import DIAGEOGAGenerator


__author__ = 'Yasmin'


class DIAGEOGACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGAGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoga-sand calculations')
#     Config.init()
#     project_name = 'diageoga'
#     data_provider = KEngineDataProvider(project_name)
#     session = '00433ECA-FB20-4CAF-B44B-717507AAC529'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGACalculations(data_provider, output).run_project_calculations()
