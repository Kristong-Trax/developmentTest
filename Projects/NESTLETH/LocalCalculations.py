
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLETH.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleth calculations')
    Config.init()
    project_name = 'nestleth'
    data_provider = KEngineDataProvider(project_name)
    session = '340AEC9F-628A-4519-A2B5-97CCDD792F61'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
