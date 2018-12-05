
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MILLERCOORS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('millercoors-sand calculations')
    Config.init()
    project_name = 'rinielsenus'
    data_provider = KEngineDataProvider(project_name)
    session = '079a7f14-5bfd-4e5a-ab1d-530dffb568ac'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
