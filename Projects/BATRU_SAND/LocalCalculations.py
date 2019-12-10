from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU_SAND.Calculations import BATRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('BATRU calculations')
    Config.init()

    project_name = 'batru-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = \
        [
            'bbd58bd8-f62c-4639-a491-210a03a1ee6c',
        ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRU_SANDCalculations(data_provider, output).run_project_calculations()
