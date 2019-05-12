
fromhierarchyo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEORU.KPIGenerator import DIAGEORUGenerator

__author__ = 'Nimrod'


class DIAGEORUDIAGEORUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEORUGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoru calculations')
#     Config.init()
#     project_name = 'diageoru'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'e4f2c2a9-820f-4d59-bb88-54eac8fdf014'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEORUDIAGEORUCalculations(data_provider, output).run_project_calculations()
