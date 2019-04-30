
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKSG_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gsksg calculations')
    Config.init()
    project_name = 'gsksg-sand2'
    data_provider = KEngineDataProvider(project_name)
    session = '5F2C8C19-7E4B-42C0-B59E-B3CBAD54678B'
    # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
