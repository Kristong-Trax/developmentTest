
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOCO_SAND.Calculations import DIAGEOCO_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageoco-sand calculations')
    Config.init()
    project_name = 'diageoco-sand'
    # sessions = [#'D8A8183F-3494-44C6-B3DB-D7650B625759',
               #'dac74549-89f5-48da-beaf-cba548b8698e',
               #'DB5EF07D-CB8B-4727-AD37-52FE9E44AF0B']
    sessions = [
        '8EE239A0-94D9-4C08-BA47-07E236717B22'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOCO_SANDCalculations(data_provider, output).run_project_calculations()
