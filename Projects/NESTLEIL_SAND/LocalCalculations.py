
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleil-sand calculations')
    Config.init()
    project_name = 'nestleil-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'da29d8b5-bc5c-4b89-b5e1-8caa4cdfc6be'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
