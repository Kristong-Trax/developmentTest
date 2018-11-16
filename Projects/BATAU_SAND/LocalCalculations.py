
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BATAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('batau-sand calculations')
    Config.init()
    project_name = 'batau-sand'
    data_provider = KEngineDataProvider(project_name)
    #session = 'f90ad7a0-23c5-46d8-9abb-f9ba04806ff9'
    session = 'a59be007-5c89-4f52-8389-086afa802dc8'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
