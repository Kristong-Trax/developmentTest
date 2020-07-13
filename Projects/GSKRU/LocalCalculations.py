
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKRU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gskru calculations')
    Config.init()
    project_name = 'gskru'
    data_provider = KEngineDataProvider(project_name)
    session = 'E7D1405E-7128-4FDC-B38A-298AA531899A'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
