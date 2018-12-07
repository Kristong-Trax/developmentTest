
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.INBEVBR.KPIGenerator import INBEVBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class INBEVBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbr calculations')
#     Config.init()
#     project_name = 'InbevBr'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     list_sessions = [
#         'f1169d5e-7bc0-4dc0-bc1c-678dbd36276d',
#         'ebeaf3fa-ca89-4876-b76e-4788cac3a60d',
#         '38a402f2-7d0f-4755-a587-75b5b5ddfe21',
#         '725c4ac1-7a42-4c78-a186-7f25ba93536d',
#     ]
#
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         INBEVBRCalculations(data_provider, output).run_project_calculations()
