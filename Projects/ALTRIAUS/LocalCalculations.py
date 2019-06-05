
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('altriaus calculations')
    Config.init()
    project_name = 'altriaus'
    sessions = [
        "A5629BC3-F3BD-4E16-BAA6-2DD046151C80",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
