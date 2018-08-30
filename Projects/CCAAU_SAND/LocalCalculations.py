
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCAAU_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccaau-sand calculations')
    Config.init()
    project_name = 'ccaau-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'b8b52f67-c986-479a-ab1f-8c5f82481327'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
