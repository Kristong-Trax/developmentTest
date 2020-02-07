
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CBCDAIRYIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcdairyil calculations')
    Config.init()
    project_name = 'cbcdairyil'
    data_provider = KEngineDataProvider(project_name)
    session = '8a51c3ec-2006-4c13-aa3c-5d021c4a570f'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
