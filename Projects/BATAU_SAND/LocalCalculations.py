
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BATAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('batau-sand calculations')
    Config.init()
    project_name = 'batau-sand'
    data_provider = KEngineDataProvider(project_name)
    #session = '84751c4f-cb52-4863-b528-5531c7d9b4f0'
    #session = 'f90ad7a0-23c5-46d8-9abb-f9ba04806ff9'
    #session = '0DB58E6A-E7AB-48B7-A99B-536882F40E61'
    session = '8D338894-6618-4516-9DC9-36745F5553CF'
    #session = 'a59be007-5c89-4f52-8389-086afa802dc8'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()