from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBENELUX.KPIGenerator import DIAGEOBENELUXGenerator

__author__ = 'Elyashiv'


class DIAGEOBENELUXCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOBENELUXGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageobenelux calculations')
#     Config.init()
#     project_name = 'diageobenelux'
#     data_provider = KEngineDataProvider(project_name)
#     session = '7639A153-6410-47C9-94CA-F321D4A5375E'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOBENELUXCalculations(data_provider, output).run_project_calculations()
