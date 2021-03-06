
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PERNODUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pernodus-sand calculations')
    Config.init()
    project_name = 'pernodus-sand'
    data_provider = KEngineDataProvider(project_name)
    session = ''
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
