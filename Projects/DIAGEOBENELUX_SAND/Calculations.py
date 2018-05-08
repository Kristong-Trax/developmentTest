from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBENELUX_SAND.KPIGenerator import DIAGEOBENELUXGenerator

__author__ = 'Elyashiv'


class DIAGEOBENELUX_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOBENELUXGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageobenelux-sand calculations')
#     Config.init()
#     project_name = 'diageobenelux-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'BE315C13-D09B-4184-A63C-61C35658937B'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOBENELUX_SANDCalculations(data_provider, output).run_project_calculations()
