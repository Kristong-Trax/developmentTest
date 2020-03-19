
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
        '200ebbf0-c901-4c7c-997c-d05bf00366a3',
    ]

    for session in sessions:
        print('=============================={}==============================='.format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
