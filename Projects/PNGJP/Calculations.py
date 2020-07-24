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
#     sessions = ["04EBF250-EAA3-4761-AA14-925C5C50E089", "00261BB8-C9B4-4AEE-8173-81D5281E6ADA"]
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGJPCalculations(data_provider, output).run_project_calculations()