
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS.Calculations import DIAGEOUSCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous calculations')
    Config.init()
    project_name = 'diageous'
    sessions = [
        "F4F21DE9-5A5F-4F7A-A8B0-423134314B20",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOUSCalculations(data_provider, output).run_project_calculations()
