from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGJP.KPIGenerator import PNGJPGenerator

__author__ = 'Nimrod'


class PNGJPCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGJPGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('pngjp calculations')
#     Config.init()
#     project_name = 'pngjp'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'FF92150A-8246-4753-BE0B-6A820A51D879'
#     # session = 'B3456AAE-50B3-49D0-B5CD-FBF6244DB57C'
#     session = '735A01C1-A05F-4891-922B-1118D6DB97C3'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGJPCalculations(data_provider, output).run_project_calculations()