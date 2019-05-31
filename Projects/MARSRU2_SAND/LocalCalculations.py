from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU2_SAND.Calculations import MARSRU2_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru2-sand'
    session_uids = [
        'ff8641db-dfdf-4cba-9fd2-7a723d82f88d',
        'fffd3e66-882a-4a12-a284-5b5851ac9ee3'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU2_SANDCalculations(data_provider, output).run_project_calculations()
