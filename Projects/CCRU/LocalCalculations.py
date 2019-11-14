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
            'FFA18EA4-4497-462F-A76B-9680D17122B4',

            # 'fff389cd-5843-4d28-ada5-d84045969649',

            # 'FF05F183-4FBC-48D0-9F78-A418EA8AEE90',

            # 'ff72196b-b28a-46c4-8b25-343380337373',
            # 'FF5CD17D-EE7D-4BC7-BD92-218CCFF4C40B',
            # 'ff5a9495-b910-4ffe-b526-69f7c7348d09',
            # 'fee82d47-2114-4f83-ab1d-3c46775056eb',
            # 'febce9d6-25f1-4830-88cd-c8c80b15c50c',
            # 'fe9462f2-ace9-493e-b08e-f8289ed3feed',
            # 'fe9261f2-9c05-48e1-8f9b-0537b73576da',
            # 'FE7809D7-6658-4AB4-A1EF-7EB9D6C7AE4B',
            # 'fe7711dd-95cc-4db1-a42f-cff1442351ec',

            # 'ff5a9495-b910-4ffe-b526-69f7c7348d09',
            # 'ff3daa13-0e4b-49dc-9241-55626ba7c779',
            # 'ff3ab26f-d43f-416e-a2f8-44727ddc6588',
            # 'FEFEB55D-A571-42B3-ABE4-87F887FB7A00',

        ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()

