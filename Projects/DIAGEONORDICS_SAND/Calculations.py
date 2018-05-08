from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEONORDICS_SAND.KPIGenerator import DIAGEONORDICSGenerator

__author__ = 'Elyashiv'


class DIAGEONORDICS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEONORDICSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageonordics-sand calculations')
#     Config.init()
#     project_name = 'diageonordics-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0617B18A-C043-490C-B9F5-019BC889A225'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEONORDICS_SANDCalculations(data_provider, output).run_project_calculations()
