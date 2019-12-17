
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript


from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'satya'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
#     from Trax.Utils.Conf.Configuration import Config
#     from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#     LoggerInitializer.init('DIAGEOIN calculations')
#     Config.init()
#     project_name = 'diageoin'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['fe4cdf53-dab1-4b6d-b640-0d631110e763']
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOINCalculations(data_provider, output).run_project_calculations()
