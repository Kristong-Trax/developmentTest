
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
        '20722d51-5306-4a96-9ad6-858a8422f967',
    ]

    for session in sessions:
        print('=============================={}==============================='.format(session))
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
