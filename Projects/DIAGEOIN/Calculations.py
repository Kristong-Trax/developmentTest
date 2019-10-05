
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'satya'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('DIAGEOIN calculations')
#     Config.init()
#     project_name = 'diageoin'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['ed1efe60-c9a5-4539-ac33-1e343ae1ed5e',
#                 '004af24c-9b85-4bef-866c-841759e4b3a2',
#                 '326eb057-f50f-4902-854d-ca0671abbf08']
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         DIAGEOINCalculations(data_provider, output).run_project_calculations()
