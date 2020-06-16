
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKJP_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gskjp-sand calculations')
    Config.init()
    project_name = 'gskjp-sand'
    data_provider = KEngineDataProvider(project_name)
    session ='9062B84C-8C3B-40FC-87F1-CF905D5EF14A'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
