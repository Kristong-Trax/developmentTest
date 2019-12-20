from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU.Calculations import BATRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('BATRU calculations')
    Config.init()

    project_name = 'batru'
    data_provider = KEngineDataProvider(project_name)
    sessions = \
        [
            'ebdc4ebb-f020-48e8-89f4-0bd9b3803bf7'
        ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
