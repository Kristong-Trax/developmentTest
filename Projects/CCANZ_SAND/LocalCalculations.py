
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCANZ_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccanz-sand calculations')
    Config.init()
    project_name = 'ccanz-sand'
    data_provider = KEngineDataProvider(project_name)

    session = '6368C810-77DB-4C2D-BD56-F62A0AE0CF60'

    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
