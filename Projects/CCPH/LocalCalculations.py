
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCPH.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccphl calculations')
    Config.init()
    project_name = 'ccphl'
    data_provider = KEngineDataProvider(project_name)
    session = '0b2aa835-c9e1-40d5-bc02-69d3bb73db1e'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
