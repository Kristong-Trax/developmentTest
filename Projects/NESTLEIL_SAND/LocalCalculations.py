
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleil-sand calculations')
    Config.init()
    project_name = 'nestleil-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'ea98ee98-58ec-4b6b-b2d6-9f205c480ff3'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
