
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CBCDAIRYIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcdairyil calculations')
    Config.init()
    project_name = 'cbcdairyil'
    data_provider = KEngineDataProvider(project_name)
    session = '5f5a9190-4cb6-4e50-a5ed-287098dcfe80'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
