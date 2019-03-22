
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEBAKINGUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestlebakingus calculations')
    Config.init()
    project_name = 'nestlebakingus'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
        'cc440e13-1b5a-4cd3-9393-ca8bc27158c7',
        'efa1c225-7832-4a69-a848-f7dd4a7babe8',
        '18e08388-557e-4836-ab1a-2f1e4a4c9a8e',
        'b26a44b0-1b7a-4724-852c-450dfc7cf8ff',
        'e601289a-5475-43a0-949f-77f13a226676',
        'c16a905f-a827-49e1-bb28-228b0936539f',
        '930d9f86-4e06-427e-89ec-3bb349bcb9cf',
        '10a5d703-78d7-4425-805f-a4228378cec3',
        '10d946c8-ae52-47c2-9399-0e036c1ab886',
        '74285f9c-5268-4159-96ee-15ce05671b05',
        'a193744c-f30e-495f-8a9c-b3cfc31264b2',
        '252b6ffd-073a-4fc5-a190-b8da732c5b86',
        '7cc6cd01-36fd-4617-a565-c13a70b55d91',
        'e9ed5ab7-3189-46d6-9a64-e1278fd493f6',
        '88eae4c7-81c6-4aa2-ac01-660da22f049b',
        'c1cf9776-1042-4f54-ab10-641cc6224c3c',
        '4b94c74e-1d70-4763-ad1b-2b113cd65004'
    ]

    for session in sessions:
        print('=============================={}==============================='.format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
