
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKSG_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gsksg calculations')
    Config.init()
    project_name = 'gsksg-sand2'
    data_provider = KEngineDataProvider(project_name)
    session = 'F9EA8B2C-8C76-43E7-9AA0-D852F307736F'
    # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
