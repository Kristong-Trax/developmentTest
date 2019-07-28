
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGH.KPIGenerator import DiageoGHGenerator


class DIAGEOGHCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DiageoGHGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageogh calculations')
#     Config.init()
#     project_name = 'diageogh'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'ff653a3e-2a29-42cc-a54b-8670f746c8ab'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOGHCalculations(data_provider, output).run_project_calculations()
