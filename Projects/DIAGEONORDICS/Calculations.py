from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEONORDICS.KPIGenerator import DIAGEONORDICSDIAGEONORDICSGenerator

__author__ = 'Elyashiv'


class DIAGEONORDICSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEONORDICSDIAGEONORDICSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageonordics-sand calculations')
#     Config.init()
#     project_name = 'diageonordics'
#     data_provider = KEngineDataProvider(project_name)
#     session = '33d10093-f21a-4f7f-b5f8-0e71c9e9fdf1'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEONORDICSCalculations(data_provider, output).run_project_calculations()
