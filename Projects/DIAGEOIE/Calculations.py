from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOIE.KPIGenerator import DIAGEOIEGenerator


class DIAGEOIECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOIEGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoie calculations')
#     Config.init()
#     project_name = 'diageoie'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'C2D8A9DC-C94D-4C24-B10A-36F3BC61E6FB'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOIECalculations(data_provider, output).run_project_calculations()
