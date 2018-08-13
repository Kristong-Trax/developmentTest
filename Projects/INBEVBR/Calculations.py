
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

#
# if __name__ == '__main__':
#     LoggerInitializer.init('inbevbr calculations')
#     Config.init()
#     project_name = 'InbevBr'
#     data_provider = KEngineDataProvider(project_name)
#     output = Output()
#
#     # second report
#     list_sessions = [
#         '9e7c9731-e9b3-4b1b-9796-5f415c0ce83d',
#         '7d8e122e-31e3-49c3-9cca-068acd809e4c',
#         '55a30d55-ca09-4233-8578-22df4a32b563',
#
#     ]
#
#
#     for session in list_sessions:
#         data_provider.load_session_data(session)
#         INBEVBRCalculations(data_provider, output).run_project_calculations()
