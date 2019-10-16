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
            'ffcfe108-0980-4da5-9db2-e3643cffeff3'
        ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRU_SANDCalculations(data_provider, output).run_project_calculations()
