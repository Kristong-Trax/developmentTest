from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU.Calculations import CCRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru'
    data_provider = KEngineDataProvider(project_name)
    session_uids = \
        [

            '025a8eb6-82fb-4aa1-991d-1c0e747b44e4',
            '66c67e9d-137f-4ece-91d7-82f7d9ffecce',
            '874d5ddb-c8bc-4a45-9f01-97dfa331b856',
            '306b39d1-5a39-409f-b371-f0395a375535',


        ]
    for session in session_uids:
        print(session)
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()

