from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOMX.KPIGenerator import DIAGEOMXGenerator


class DIAGEOMXCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOMXGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx calculations')
#     Config.init()
#     project_name = 'diageomx'
#     data_provider = KEngineDataProvider(project_name)
#     session = '7e5b72d5-7678-47fb-86a5-dc6f8ad0ccac'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOMXCalculations(data_provider, output).run_project_calculations()

