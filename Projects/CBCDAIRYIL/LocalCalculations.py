
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CBCDAIRYIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcdairyil calculations')
    Config.init()
    project_name = 'cbcdairyil'
    data_provider = KEngineDataProvider(project_name)
    session = '26a84205-a169-431a-8227-90a17585a59d'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
