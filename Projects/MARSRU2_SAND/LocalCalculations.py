from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU2_SAND.Calculations import MARSRU2_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru2-sand'
    session_uids = [
        'ff23ea8d-2e26-4c03-9ac2-3dccbc6ed72c',
        'ff959c2c-cc64-464f-ad1b-0c272530abc4',
        'fffd3e66-882a-4a12-a284-5b5851ac9ee3'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU2_SANDCalculations(data_provider, output).run_project_calculations()
