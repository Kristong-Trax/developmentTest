
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCKR_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cckr-sand calculations')
    Config.init()
    project_name = 'cckr-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '895a4d8f-3523-4131-94f9-ac2ac364ddd9'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
