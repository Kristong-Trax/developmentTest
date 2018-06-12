
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Projects.INBEVBR_SAND.KPIGenerator import INBEVBRGenerator
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'ilays'


class INBEVBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        INBEVBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('inbevbr-sand calculations')
    Config.init()
    project_name = 'inbevbr-sand'
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    list_sessions = [ '41a0ff56-4cea-421b-8d2e-34984b439d7d',
                    '0bbadf52-d621-4859-ae94-466a6ecafcb4',
                    'c1190c08-4db0-4d85-b795-3512cabedc1a',
                    'b02fb67f-63d8-42c0-9a7d-673aff37c248']
    for session in list_sessions:
        data_provider.load_session_data(session)
        INBEVBRCalculations(data_provider, output).run_project_calculations()
