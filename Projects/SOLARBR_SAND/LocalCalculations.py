
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SOLARBR_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('solarbr-sand calculations')
    Config.init()
    project_name = 'solarbr-sand'
    data_provider = KEngineDataProvider(project_name)
    session =  'a93ba740-dc13-4a49-8f14-ef9b4693c8a3'


    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
