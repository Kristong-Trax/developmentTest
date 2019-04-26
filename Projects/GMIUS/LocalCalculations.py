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
    sessions = ['01311ede-dd27-413b-9ae5-73a4c56fe5b2']

    sessions = [
        # 'd0e02332-b182-4ec7-9776-f79ae47fc8a6',
        # 'ab70f0f1-7ea1-4e76-a21e-45b45809d301',

        # '342c2485-28ee-46d2-8d6b-11f355f574d2',
        # '3726b021-6bf6-40c2-aca3-2c8a28189cf1',
        # 'a01facf5-992d-462d-bfed-efc676bca6cb',
        # '5ebee0e0-0bf6-433a-a702-76cd98a0ee0e',
        #  'ddce9cfd-63cc-444a-b37a-5aadda6fd695',
        # '4a1b84c6-8ce1-4f38-a305-036bafaffb78',
    ]

    # sessions = ['00ee5871-5fe6-4f27-8963-f2b5fb4badd2']
    sessions = ['014bd0ba-72b2-47bd-abe1-ffd55d1f2af7']

    sessions = [
        '000153d5-256d-434e-9575-0c231aa58c90',
        '002ba7b9-80a7-4ffa-947f-c8e6a885bda6',
        '00490877-3e80-47be-a275-9d37b1c1d2ec',
        '007714b9-a0a0-44f8-9480-480d834f7836',
        '00805381-6efc-4cdb-adf9-13e097d8c6f5',
        '00850ba9-da47-4a18-8faf-b541404c1f48',
        '00ee5871-5fe6-4f27-8963-f2b5fb4badd2',
        '01311ede-dd27-413b-9ae5-73a4c56fe5b2',
        '01450398-4bb3-4a66-8d22-2be2bd7e0f24',
        '014bd0ba-72b2-47bd-abe1-ffd55d1f2af7',
        '01714d57-3a67-4381-9c96-15fb417539d3',
        '01eeca26-9c67-4960-b5c9-c92cbf9dec69',

    ]

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

