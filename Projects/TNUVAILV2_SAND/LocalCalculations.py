
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TNUVAILV2_SAND.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('tnuvailv2-sand calculations')
    Config.init()
    project_name = 'tnuvailv2-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '2c9f39fb-46e2-46a3-8fc3-3de48958a34c'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
