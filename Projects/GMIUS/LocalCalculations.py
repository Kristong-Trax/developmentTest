from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GMIUS.Calculations import Calculations

if __name__ == '__main__':
    LoggerInitializer.init('gmius calculations')
    Config.init()
    project_name = 'gmius'
    sessions = [
                'ab70f0f1-7ea1-4e76-a21e-45b45809d301',
                '039907ec-6e6f-4d41-b1a1-595152e21225',
                '342c2485-28ee-46d2-8d6b-11f355f574d2',
                '70ec27f5-457c-4513-8927-697450c4cbe5',
                '3726b021-6bf6-40c2-aca3-2c8a28189cf1',
                'a01facf5-992d-462d-bfed-efc676bca6cb',
                '5ebee0e0-0bf6-433a-a702-76cd98a0ee0e',
                '4a1b84c6-8ce1-4f38-a305-036bafaffb78',
                'bb320b40-016f-40b0-8eea-225c81815724',
                '936360d6-9b90-40b4-9af9-4841312a9a1b',
                '52b8ca84-2756-4396-9058-9a3ad5c4cccb',
                '5aa81aa6-1bab-4730-8441-81ffb5513d11',
                '5a769904-58aa-42b3-bd8b-7f76fd8fd43a',
                '5f3e26ef-eafc-4729-b18d-51b8799a71be',
                '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
                '26175b23-f786-4564-a9a4-e8810ce231d9',
                  ]
    # sessions = ['936360d6-9b90-40b4-9af9-4841312a9a1b']
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

