
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.DIAGEOAU_SAND.KPIGenerator import DiageoAUSandGenerator


class DiageoAUSandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DiageoAUSandGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageoau-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '57F997DA-155F-4F50-9E49-B02C328CCD95'
#     data_provider.load_session_data(session)
#     output = Output()
#     DiageoAUSandGenerator(data_provider, output).run_project_calculations()
