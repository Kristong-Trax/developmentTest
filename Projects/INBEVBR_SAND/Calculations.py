
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
    list_sessions = [
                        # 'fb74f5f1-4949-4178-846f-1bbb5599f80c',
                        # 'e78cb48d-7f87-4aaf-b86d-eb90fefcaab1',
                        # 'db0a4274-4f9b-4869-a1a9-00e69685e2de',
                        'd9fc66f5-4586-4353-8ca0-2f7ab82b86c7',
                        'd8a74726-66c8-4a6d-b96e-12c88b503dd8',
                        'd3f28d87-8e04-4f79-ab8e-e818d60af75d',
                        'd13fa43f-ac29-4a89-b7a1-ec32aee2fe57',
                        'caac33af-e1a9-42f8-a9d3-2f1e27aee7ec',
                        'c1190c08-4db0-4d85-b795-3512cabedc1a',
                        'bb67315c-b1a4-4fa4-8589-30d943a55711',
                        'b50b04e3-8f45-45b1-9aec-bc4cc79761c3',
                        'b0598ad1-0139-4565-b9a0-73149ea444b5',
                        'b02fb67f-63d8-42c0-9a7d-673aff37c248',
                        'af8bc774-58ba-4cb2-8781-b47634bf1d29',
                        'a7e123dc-1736-4d7e-8f5d-9a916c941c7f',
                        '9ffad16b-a600-4d54-8069-767bc3f08d58',
                        '9c911310-a611-4302-8109-b8f77f281741',
                        '94791803-abb5-49e7-a797-e13a8ed64111',
                        '91f52fe9-31e6-404e-8314-f5fcd761dcc7',
                        '8fa0b79a-ee22-421a-aaaa-89ddf011cea2']
    for session in list_sessions:
        data_provider.load_session_data(session)
        INBEVBRCalculations(data_provider, output).run_project_calculations()
