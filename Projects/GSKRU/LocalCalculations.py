from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKRU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init()
    project_name = 'gskru'
    sessions = [

        'f98497c6-1b30-4105-841b-0eafec5a7a9f',

        # 'f98497c6-1b30-4105-841b-0eafec5a7a9f',
        # 'aca2d140-d7dc-4817-9a42-95d2a69c70ff',
        # 'c61f1054-e11f-41c3-b21e-54f7a4f776d7',
        # '993cff34-c1b3-4659-95f5-44de06468f07',
        # '87873358-fcb2-40d6-b68d-f59c07242e76',
        # 'e161137e-5943-427c-acc2-2ecd30d0e16d',
        # 'fea0f508-cfa5-4968-be68-256575a122ea'

        ]

    for session in sessions:
        print session
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
