from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GOOGLEKR.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('googlekr calculations')
    Config.init()
    project_name = 'googlekr'
    sessions = ["8e69ef91-1275-42ba-82a3-12a775683fb6"]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

