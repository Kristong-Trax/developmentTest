from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PS1_SAND.KPIGenerator import Generator


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('ps1-sand calculations')
#     Config.init()
#     project_name = 'ps1-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '39600BB2-2891-43DD-8A80-ACDF9DD514FC'
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
