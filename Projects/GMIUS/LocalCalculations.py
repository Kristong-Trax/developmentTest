from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GMIUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gmius calculations')
    Config.init()
    project_name = 'gmius'


    # sessions = ['0a3eba32-61b9-4b71-b3a2-692cfbf8cec9']
    # sessions = ['7da5fee8-06b9-4e32-8382-e8379de2ae51']
    sessions = ['224b07f0-a4e2-4375-890d-bc28855006d1']
    sessions = ['04771142-03b9-4e27-a074-45a8688a187b']

    sessions = [
        # 'd0e02332-b182-4ec7-9776-f79ae47fc8a6',
        # 'ab70f0f1-7ea1-4e76-a21e-45b45809d301',
        # '039907ec-6e6f-4d41-b1a1-595152e21225',
        # '342c2485-28ee-46d2-8d6b-11f355f574d2',
        # '3726b021-6bf6-40c2-aca3-2c8a28189cf1',
        # 'a01facf5-992d-462d-bfed-efc676bca6cb',
        # '5ebee0e0-0bf6-433a-a702-76cd98a0ee0e',
        #  'ddce9cfd-63cc-444a-b37a-5aadda6fd695',
        # '4a1b84c6-8ce1-4f38-a305-036bafaffb78',
    ]

    sessions = ['1a622884-a5b6-440b-a976-1292834b355c']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

