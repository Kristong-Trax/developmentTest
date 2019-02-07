
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOUS.Calculations import DIAGEOUSCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageous calculations')
    Config.init()
    project_name = 'diageous'
    sessions = [
        "00F171B3-283D-476A-AE33-98B16C1BF37B",
        "009457EC-392B-4E76-992F-4AC5DC35D5EF",
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOUSCalculations(data_provider, output).run_project_calculations()
