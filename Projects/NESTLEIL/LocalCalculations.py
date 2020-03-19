
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleil calculations')
    Config.init()
    project_name = 'nestleil'
    data_provider = KEngineDataProvider(project_name)
    session = 'd4fe5efd-19b9-457a-a8c3-4bb4a6b72c88'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
