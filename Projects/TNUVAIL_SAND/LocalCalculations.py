
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TNUVAIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('tnuvail-sand calculations')
    Config.init()
    project_name = 'tnuvailv2'
    data_provider = KEngineDataProvider(project_name)
    session = '07d7b4e0-5acc-401b-807d-71221be5446b'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
