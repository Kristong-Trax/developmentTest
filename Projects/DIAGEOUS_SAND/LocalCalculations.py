
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous-sand calculations')
    Config.init()
    project_name = 'diageous-sand'
    sessions = ['0B663792-9E0B-4DC0-A604-8437CFD118D0',
                "D084B4B8-5C98-4A2A-863C-EBFA1CF5E87D",
                "A1A30A52-A79F-42A5-B7BD-0FFE53B579DB"]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
