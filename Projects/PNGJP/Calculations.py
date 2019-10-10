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
#     sessions = ["3323D6AE-BBFA-4A12-B0A2-9803DD7C52CC",
#                 "180DF2CD-7BC9-492C-A4F6-D4418DBBCBCA"]
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         PNGJPCalculations(data_provider, output).run_project_calculations()