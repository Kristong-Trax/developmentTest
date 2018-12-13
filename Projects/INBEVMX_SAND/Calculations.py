
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Projects.INBEVMX_SAND.KPIGenerator import Generator

__author__ = 'ilays'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('inbevmx-sand calculations')
#     Config.init()
#     project_name = 'inbevmx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     list_sessions = [
#         '15E6D6FD-FC45-4703-8A99-D33C805FAC41',
#         'fff20792-6a60-4a13-bb00-879a308c1ea6',
#         '341a9b53-65ad-43b6-9fe0-2ae56fcbe9bd',
#         '409c0346-a00a-4c7b-9f3b-63f4ab94307f',
#         '6a19080f-9741-4760-85fb-22a5e774d13b'
#     ]
#
#
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         Calculations(data_provider, output).run_project_calculations()

