
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGMCCN.KPIGenerator import PNGMCCNGenerator

__author__ = 'Nimrod'


class PNGMCCNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGMCCNGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngmccn calculations')
#     Config.init()
#     project_name = 'pngmccn'
#     data_provider = KEngineDataProvider(project_name)
#     session = '01b714a9-35ff-455f-8a2d-365debca8c31'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGMCCNCalculations(data_provider, output).run_project_calculations()
