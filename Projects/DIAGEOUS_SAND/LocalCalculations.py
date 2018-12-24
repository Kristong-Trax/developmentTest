
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import DIAGEOUS_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous calculations')
    Config.init()
    project_name = 'diageous-sand'
    sessions = [
        "004F1CF3-7135-44FD-9651-5F1E9E4C0BB6",
        "CFF663F6-D2C8-453C-B901-95BE44368809",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOUS_SANDCalculations(data_provider, output).run_project_calculations()
