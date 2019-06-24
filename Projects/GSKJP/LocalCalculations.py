
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gskjp calculations')
    Config.init()
    project_name = 'gskjp-sand'# working on gsk -sand
    data_provider = KEngineDataProvider(project_name)
    session = '454af254-d361-4286-8d20-dd91d7651463'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
