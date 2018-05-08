
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGMCCN_SAND.KPIGenerator import PNGMCCN_SANDGenerator

__author__ = 'Nimrod'


class PNGMCCN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGMCCN_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngmccn calculations')
#     Config.init()
#     project_name = 'pngmccn-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '01b714a9-35ff-455f-8a2d-365debca8c31'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGMCCN_SANDCalculations(data_provider, output).run_project_calculations()
