
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLETH_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestleth-sand calculations')
    Config.init()
    project_name = 'nestleth-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '83caf9c1-69b6-40d6-a7f0-944083734b33'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
