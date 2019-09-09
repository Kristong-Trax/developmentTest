
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.INBEVMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('inbevmx calculations')
    Config.init()
    project_name = 'inbevmx'
    data_provider = KEngineDataProvider(project_name)
    list_sessions = [
                    'c5f67862-62dc-4ae2-bf11-c6fc6cce04ae'
    ]
    output = Output()
    for session in list_sessions:
        data_provider.load_session_data(session)
        Calculations(data_provider, output).run_project_calculations()