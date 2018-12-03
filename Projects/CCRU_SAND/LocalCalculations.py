from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        '18D2F815-6AF4-4894-97C7-871E12E81E51',
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
