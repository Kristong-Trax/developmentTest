
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gskjp calculations')
    Config.init()
    project_name = 'gskjp'
    data_provider = KEngineDataProvider(project_name)
    session ='92C9B9CE-CD2C-4259-9035-D4DA5301168E'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
