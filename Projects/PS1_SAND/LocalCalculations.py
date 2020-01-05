
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PS1_SAND.Calculations import PS1SandCalculations


if __name__ == '__main__':
    LoggerInitializer.init('ps1-sand calculations')
    Config.init()
    project_name = 'ps1-sand'
    sessions = ['33ffe30d-1cdf-46c6-950e-4cfc7b0c9c93', '31f7ba5c-ccc2-48ab-a328-ac577ef48d39']
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        PS1SandCalculations(data_provider, output).run_project_calculations()
