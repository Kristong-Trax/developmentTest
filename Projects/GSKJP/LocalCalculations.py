
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gskjp calculations')
    Config.init()
    project_name = 'gskjp' # working on gsk -sand 
    data_provider = KEngineDataProvider(project_name)
    session = '26B868BB-A15F-43C1-ABCE-8C90A47008C8'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
