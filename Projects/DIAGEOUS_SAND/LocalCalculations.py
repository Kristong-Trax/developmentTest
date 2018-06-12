
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous'
    sessions = [
        #'004F1CF3-7135-44FD-9651-5F1E9E4C0BB6',
        "013C844D-63A4-4A67-A8C2-7CB26F875E64",
        # "2307C486-FB2F-4163-B0F9-F32ECE64A169"
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
