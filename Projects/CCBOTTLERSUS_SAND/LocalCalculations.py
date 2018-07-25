
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus-sand calculations')
    Config.init()
    project_name = 'ccbottlersus'
    data_provider = KEngineDataProvider(project_name)
    session = 'A9645825-68FF-4F19-8CE3-989B7284CB6A'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
