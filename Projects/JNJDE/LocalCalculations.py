
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSUAE.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjde calculations')
    Config.init()
    project_name = 'jnjde'
    data_provider = KEngineDataProvider(project_name)
    session = 'cb4a162c-6a44-4faa-a692-1613234672c2'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
