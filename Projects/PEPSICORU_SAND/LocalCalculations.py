
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSICORU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pepsicoru-sand calculations')
    Config.init()
    project_name = 'pepsicoru-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '022c3299-33df-4300-b3cc-001909c3e908'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
