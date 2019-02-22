
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCTRADMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cctradmx calculations')
    Config.init()
    project_name = 'cctradmx'
    data_provider = KEngineDataProvider(project_name)
    session = '6aafb584-d63a-4c51-8deb-d8f05e2b1210'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
