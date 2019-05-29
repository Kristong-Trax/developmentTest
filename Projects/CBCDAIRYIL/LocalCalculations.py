
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer, Log
from Projects.CBCDAIRYIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcdairyil calculations')
    Config.init()
    project_name = 'cbcdairyil'
    data_provider = KEngineDataProvider(project_name)
    session = 'ae2eccd5-26f9-49ac-b294-a82508c958e7'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
