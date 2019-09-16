
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSUAE_PILOT.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('marsuae calculations')
    Config.init()
    project_name = 'marsuae'
    data_provider = KEngineDataProvider(project_name)
    session = '81dc1fc5-ecbc-4bae-88d0-9abea90fce0f '
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
