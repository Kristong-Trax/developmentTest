
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGRO_SAND.KPIGenerator import PNGRO_SAND_PRODGenerator

__author__ = 'Israel'


class PNGRO_SAND_PRODCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGRO_SAND_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('pngro calculations')
    Config.init()
    project_name = 'pngro-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '9640dea8-c7ea-4a76-a63b-e9b3bf77ebe1',
        'e1826bf8-dc73-41c0-95d0-32e78e03536a',
        '1a8278df-bda6-4403-bb9f-b1eb5d26cd7f',
        'bb94b8a8-cf40-4eec-b707-166e0430a358',
        'a1f14390-0298-42ce-89ae-e9de5b12598d',
        '6cc4ef9e-9e59-4d40-a1cd-180a6a762cdc',
        'f10e4dcd-27ea-40d0-a0bf-9a976edb5114',
        '002ce221-8d77-43fd-9fdf-2a80a5a0a23f',
        '4367e501-45da-4234-99c0-f066bdbcbeb0',
        '17f62eab-b788-4772-a963-4dfe76aaff4f',
        'eb70c0c9-9f03-4f6e-9a3f-03ddf36c4bf8',
        '7630a957-3bd4-4eec-8df3-aa61c0380663',
        '08415193-82a7-4778-9588-2bd61380a730',
        'f525e0ba-2feb-4325-82d0-e2d45f160597',
        '4793e7df-0111-405c-ba9a-b0dfd6f998cf',
        'e00c255e-307d-4aa2-8fbc-cb1e8e084621',
        'ac40e5ac-fc6a-4645-9fbc-ce118c0df330',
        '9143dbbd-3fc7-4383-a3d2-01bf8959cae7',
        '651b31f2-76b9-485d-b345-8808c082ab32',
        '7e8ca2b5-b204-43be-9e25-d59a147d0d9b',
        'b0ae9059-b4ab-4fb1-a801-c721d669454a',
        '5354f943-f6fc-49ef-85a8-de315d7dea5d',
        '828e558f-93e9-429a-9621-fe9719d46045',
        '118f1c6b-91e2-4112-bce5-c235f6589893',
        '8dea17c3-a90d-4e3a-ad01-f89a61d1e91a',
        'a34bee1b-f121-4b88-88d7-1f75206dd4f3',
        '33d5370c-d8da-48be-8ba9-b3a032a385fc',
        '348ac84a-390a-435f-837e-b70e0adf2c9b',
        'd3f3bd41-f7b8-49dc-9811-6035fe49e2f2',
        'bd10adbb-4bf6-49b4-bbeb-aa8f0f45a2fd',
        '30402566-9842-47a2-83fc-7c378cd49e3d'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        PNGRO_SAND_PRODCalculations(data_provider, output).run_project_calculations()

