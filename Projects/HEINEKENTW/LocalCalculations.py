
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.HEINEKENTW.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('heinekentw calculations')
    Config.init()
    project_name = 'heinekentw'
    data_provider = KEngineDataProvider(project_name)
    session = 'A8F6603C-63D8-4C17-8826-3130AA134C5F'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
