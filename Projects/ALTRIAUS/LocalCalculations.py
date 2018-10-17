
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('altriaus calculations')
    Config.init()
    project_name = 'altriaus'
    data_provider = KEngineDataProvider(project_name)
    session = '7c46c139-cbfd-11e8-8a81-129f596660d8'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
