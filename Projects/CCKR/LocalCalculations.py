
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCKR.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cckr calculations')
    Config.init()
    project_name = 'cckr'
    data_provider = KEngineDataProvider(project_name)
    session = '63832496-6831-11e4-95c2-12e613ba0fea'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
