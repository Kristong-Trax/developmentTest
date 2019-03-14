from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru_sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        '7B425A49-4B26-42D7-894D-1F9B328934F1',
        '56b62e42-e8ee-4a4a-8cfe-02ad678bf15e'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
