
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TWEAU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('tweau calculations')
    Config.init()
    project_name = 'tweau'
    data_provider = KEngineDataProvider(project_name)
    session = '2b55a846-4689-46d0-b812-ad5586eb566b'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
