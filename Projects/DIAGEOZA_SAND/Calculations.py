
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOZA_SAND.KPIGenerator import DIAGEOZASANDGenerator

__author__ = 'Nimrod'


class DIAGEOZASANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOZASANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoza calculations')
#     Config.init()
#     project_name = 'diageoza-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '145b46cc-69c0-4b24-9b3c-1810aa6910d4'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOZASANDCalculations(data_provider, output).run_project_calculations()
